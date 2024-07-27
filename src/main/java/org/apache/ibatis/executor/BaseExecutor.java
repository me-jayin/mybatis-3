/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

/**
 * 基本的执行器，提供常用的基本操作
 *
 * @author Clinton Begin
 */
public abstract class BaseExecutor implements Executor {

    private static final Log log = LogFactory.getLog(BaseExecutor.class);

    /**
     * 事务操作对象，用于管理事务
     */
    protected Transaction transaction;
    /**
     * 被包装的执行器
     */
    protected Executor wrapper;
    /**
     * 持有延迟加载对象的 queue
     * 这里的延迟加载与懒加载不大一样，因为 mybatis 中存在嵌套查询，可能存在嵌套查询中的可能会存在嵌套查询问题
     * 1. select * from blog where id = #{blogId};
     *   1.1. select * from user where id = #{authorId}
     *      1.1.1. select * from blog where id = #{recentBlogId} # 获取用户最新文章（举例）
     * 在上述例子中，先查询博客，然后再查询博客的作者，接着再根据博客作者最新的博客文章id来查询博客信息。
     * 假如 第一步查询的博客即为作者最新发布的文章，那么第三步1.1.1 查询的博客即位第一步查询的博客。
     * 因此 mybatis 为了避免嵌套、减少重复查询的情况，使用 延迟加载。
     * 第一步查询时会添加一个 DEFERRED 标记，然后在第三步执行前，会从一级缓存（本地缓存）中获取到 DEFERRED 标记，这时将会登记到 deferredLoads 队列后直接返回，
     * 而第一步执行完成后，会用最终的对象把缓存中 DEFERRED 对象替换掉。
     * 等到所有元素都解析完成后， mybatis 再回来遍历 deferredLoads 所有的元素，进行延迟加载填充
     * 可参考 https://blog.csdn.net/weixin_44051038/article/details/121550288
     *
     * 而懒加载是通过 cglib 代理 org/apache/ibatis/executor/resultset/DefaultResultSetHandler.java:855
     * 来代理这些懒加载的对象
     */
    protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;
    /**
     * 本地缓存器，即一级缓存
     */
    protected PerpetualCache localCache;
    /**
     * 本地输出参数缓存器
     */
    protected PerpetualCache localOutputParameterCache;
    /**
     * mybatis系统配置
     */
    protected Configuration configuration;

    /**
     * 查询层数，用于记录嵌套查询数
     */
    protected int queryStack;
    /**
     * 是否关闭当前执行器
     */
    private boolean closed;

    protected BaseExecutor(Configuration configuration, Transaction transaction) {
        this.transaction = transaction;
        this.deferredLoads = new ConcurrentLinkedQueue<>();
        this.localCache = new PerpetualCache("LocalCache");
        this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
        this.closed = false;
        this.configuration = configuration;
        this.wrapper = this;
    }

    @Override
    public Transaction getTransaction() {
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        return transaction;
    }

    @Override
    public void close(boolean forceRollback) {
        try {
            try {
                rollback(forceRollback);
            } finally {
                if (transaction != null) {
                    transaction.close();
                }
            }
        } catch (SQLException e) {
            // Ignore. There's nothing that can be done at this point.
            log.warn("Unexpected exception on closing transaction.  Cause: " + e);
        } finally {
            transaction = null;
            deferredLoads = null;
            localCache = null;
            localOutputParameterCache = null;
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * 更新操作，会清空本地缓存后，调用 doUpdate 方法执行更新操作
     *
     * @param ms
     * @param parameter
     * @return
     * @throws SQLException
     */
    @Override
    public int update(MappedStatement ms, Object parameter) throws SQLException {
        ErrorContext.instance().resource(ms.getResource()).activity("executing an update").object(ms.getId());
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        clearLocalCache();
        return doUpdate(ms, parameter);
    }

    @Override
    public List<BatchResult> flushStatements() throws SQLException {
        return flushStatements(false);
    }

    public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        return doFlushStatements(isRollBack);
    }

    /**
     * 基础查询
     *
     * @param ms
     * @param parameter
     * @param rowBounds
     * @param resultHandler
     * @param <E>
     * @return
     * @throws SQLException
     */
    @Override
    public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler)
            throws SQLException {
        // 获取 BoundSql
        BoundSql boundSql = ms.getBoundSql(parameter);
        CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
        return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
    }

    /**
     * 最终执行方法
     *
     * @param ms
     * @param parameter     入参
     * @param rowBounds     RowBounds 行限制
     * @param resultHandler 结果处理器
     * @param key           缓存键
     * @param boundSql      实际sql描述对象
     * @param <E>
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    @Override
    public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler,
                             CacheKey key, BoundSql boundSql) throws SQLException {
        ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        // 如果是主查询，并且需要清空缓存，则执行前先清除
        if (queryStack == 0 && ms.isFlushCacheRequired()) {
            clearLocalCache();
        }
        List<E> list;
        try {
            queryStack++;
            // 如果不是 ResultHandler 处理结果集的方式，则直接从本地缓存中获取值
            list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;
            if (list != null) { // 如果存在缓存，则需要针对 StatementType 为 CALLABLE 的进行特殊处理
                handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
            } else { // 无缓存则直接查询
                list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
            }
        } finally {
            queryStack--;
        }
        if (queryStack == 0) {
            // 处理延迟加载
            for (DeferredLoad deferredLoad : deferredLoads) {
                deferredLoad.load();
            }
            // issue #601
            deferredLoads.clear();

            // 如果本地缓存级别是 STATEMENT，则清空缓存，表示该缓存只在当前 Statement 有效
            if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
                clearLocalCache();
            }
        }
        return list;
    }

    @Override
    public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
        BoundSql boundSql = ms.getBoundSql(parameter);
        return doQueryCursor(ms, parameter, rowBounds, boundSql);
    }

    /**
     * 记录延迟加载对象
     * @param ms
     * @param resultObject
     * @param property
     * @param key
     * @param targetType
     */
    @Override
    public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key,
                          Class<?> targetType) {
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
        if (deferredLoad.canLoad()) {
            deferredLoad.load();
        } else {
            deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
        }
    }

    /**
     * 创建select查询的缓存键
     *
     * @param ms
     * @param parameterObject
     * @param rowBounds
     * @param boundSql
     * @return
     */
    @Override
    public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
        if (closed) {
            throw new ExecutorException("Executor was closed.");
        }
        CacheKey cacheKey = new CacheKey();
        cacheKey.update(ms.getId());
        cacheKey.update(rowBounds.getOffset());
        cacheKey.update(rowBounds.getLimit());
        cacheKey.update(boundSql.getSql());
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
        // mimic DefaultParameterHandler logic
        MetaObject metaObject = null;
        for (ParameterMapping parameterMapping : parameterMappings) {
            if (parameterMapping.getMode() != ParameterMode.OUT) {
                Object value;
                String propertyName = parameterMapping.getProperty();
                if (boundSql.hasAdditionalParameter(propertyName)) {
                    value = boundSql.getAdditionalParameter(propertyName);
                } else if (parameterObject == null) {
                    value = null;
                } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                    value = parameterObject;
                } else {
                    if (metaObject == null) {
                        metaObject = configuration.newMetaObject(parameterObject);
                    }
                    value = metaObject.getValue(propertyName);
                }
                cacheKey.update(value);
            }
        }
        if (configuration.getEnvironment() != null) {
            // issue #176
            cacheKey.update(configuration.getEnvironment().getId());
        }
        return cacheKey;
    }

    @Override
    public boolean isCached(MappedStatement ms, CacheKey key) {
        return localCache.getObject(key) != null;
    }

    @Override
    public void commit(boolean required) throws SQLException {
        if (closed) {
            throw new ExecutorException("Cannot commit, transaction is already closed");
        }
        clearLocalCache();
        flushStatements();
        if (required) {
            transaction.commit();
        }
    }

    @Override
    public void rollback(boolean required) throws SQLException {
        if (!closed) {
            try {
                clearLocalCache();
                flushStatements(true);
            } finally {
                if (required) {
                    transaction.rollback();
                }
            }
        }
    }

    /**
     * 清除本地缓存
     */
    @Override
    public void clearLocalCache() {
        if (!closed) {
            localCache.clear();
            localOutputParameterCache.clear();
        }
    }

    protected abstract int doUpdate(MappedStatement ms, Object parameter) throws SQLException;

    protected abstract List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException;

    /**
     * 执行查询操作
     * @param ms
     * @param parameter
     * @param rowBounds
     * @param resultHandler
     * @param boundSql
     * @return
     * @param <E>
     * @throws SQLException
     */
    protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                           ResultHandler resultHandler, BoundSql boundSql) throws SQLException;

    protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                                   BoundSql boundSql) throws SQLException;

    protected void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Apply a transaction timeout.
     *
     * @param statement a current statement
     * @throws SQLException if a database access error occurs, this method is called on a closed <code>Statement</code>
     * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
     * @since 3.4.0
     */
    protected void applyTransactionTimeout(Statement statement) throws SQLException {
        StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
    }

    /**
     * 如果是 CALLABLE 存储过程调用的方式，则需要使用缓存来设置 OUT 参数的值
     * @param ms
     * @param key
     * @param parameter
     * @param boundSql
     */
    private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter,
                                                     BoundSql boundSql) {
        if (ms.getStatementType() == StatementType.CALLABLE) {
            // 从output参数缓存中获取存储过程output参数
            final Object cachedParameter = localOutputParameterCache.getObject(key);
            if (cachedParameter != null && parameter != null) {
                final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
                final MetaObject metaParameter = configuration.newMetaObject(parameter);
                // 遍历ParameterMapping中对OUT参数的描述，并将 缓存 中对应属性的值设置到OUT参数中
                for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
                    if (parameterMapping.getMode() != ParameterMode.IN) {
                        final String parameterName = parameterMapping.getProperty();
                        final Object cachedValue = metaCachedParameter.getValue(parameterName);
                        metaParameter.setValue(parameterName, cachedValue);
                    }
                }
            }
        }
    }

    /**
     * 从数据库中查询数据
     * @param ms
     * @param parameter
     * @param rowBounds
     * @param resultHandler
     * @param key
     * @param boundSql
     * @return
     * @param <E>
     * @throws SQLException
     */
    private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                          ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
        List<E> list;
        // 添加执行中的占位对象，此处代码的作用就是为了防止嵌套查询是查询了相同的数据
        // 举个例子，一个Blog有一个Author，而Author中又嵌套了一个Blog，那么Blog还没有放到缓存中，但是嵌套查询现在查Author，
        // Author中的Blog又是第一个Blog查询的数据，这里放置一个占位符就是为了说明，这个Blog已经在查询了，结果还没出来而已，不要急，等结果出来了再进行配对
        localCache.putObject(key, EXECUTION_PLACEHOLDER);
        try {
            list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
        } finally {
            localCache.removeObject(key);
        }
        // 添加最终的缓存对象
        localCache.putObject(key, list);
        // 如果是 CALLABLE 类型 StatementType，则需要把 入参 给缓存，因为 CALLABLE 中 OUT 类型的参数会直接修改 入参 的值
        if (ms.getStatementType() == StatementType.CALLABLE) {
            localOutputParameterCache.putObject(key, parameter);
        }
        return list;
    }

    /**
     * 获取连接对象
     *
     * @param statementLog
     * @return
     * @throws SQLException
     */
    protected Connection getConnection(Log statementLog) throws SQLException {
        // 跳过事务管理器获取jdbc连接
        Connection connection = transaction.getConnection();
        // 如果使用 debug 日志级别，则对 Connection 对象进行代理，从而实现方法调用的日志输出
        if (statementLog.isDebugEnabled()) {
            return ConnectionLogger.newInstance(connection, statementLog, queryStack);
        }
        return connection;
    }

    @Override
    public void setExecutorWrapper(Executor wrapper) {
        this.wrapper = wrapper;
    }

    /**
     * 延迟加载对象，持有延迟加载操作的描述信息
     */
    private static class DeferredLoad {
        /**
         * 持有延迟属性的对象
         */
        private final MetaObject resultObject;
        /**
         * 延迟属性
         */
        private final String property;
        /**
         * 该延迟属性类型
         */
        private final Class<?> targetType;
        /**
         * 缓存key
         */
        private final CacheKey key;
        /**
         * 本地缓存器
         */
        private final PerpetualCache localCache;
        /**
         * 创建对象的工厂
         */
        private final ObjectFactory objectFactory;
        /**
         * 结果提取器，对查询后的结果进行处理
         */
        private final ResultExtractor resultExtractor;

        // issue #781
        public DeferredLoad(MetaObject resultObject, String property, CacheKey key, PerpetualCache localCache,
                            Configuration configuration, Class<?> targetType) {
            this.resultObject = resultObject;
            this.property = property;
            this.key = key;
            this.localCache = localCache;
            this.objectFactory = configuration.getObjectFactory();
            this.resultExtractor = new ResultExtractor(configuration, objectFactory);
            this.targetType = targetType;
        }

        public boolean canLoad() {
            return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
        }

        public void load() {
            @SuppressWarnings("unchecked")
            // we suppose we get back a List
            List<Object> list = (List<Object>) localCache.getObject(key);
            Object value = resultExtractor.extractObjectFromList(list, targetType);
            resultObject.setValue(property, value);
        }

    }

}
