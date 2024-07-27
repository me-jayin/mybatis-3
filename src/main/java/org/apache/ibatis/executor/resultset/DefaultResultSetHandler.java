/*
 *    Copyright 2009-2024 the original author or authors.
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
package org.apache.ibatis.executor.resultset;

import org.apache.ibatis.annotations.AutomapConstructor;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.cursor.defaults.DefaultCursor;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.loader.ResultLoader;
import org.apache.ibatis.executor.loader.ResultLoaderMap;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.executor.result.DefaultResultHandler;
import org.apache.ibatis.executor.result.ResultMapException;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.*;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.apache.ibatis.util.MapUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Iwao AVE!
 * @author Kazuki Shimizu
 */
public class DefaultResultSetHandler implements ResultSetHandler {

    private static final Object DEFERRED = new Object();

    private final Executor executor;
    private final Configuration configuration;
    private final MappedStatement mappedStatement;
    private final RowBounds rowBounds;
    private final ParameterHandler parameterHandler;
    private final ResultHandler<?> resultHandler;
    private final BoundSql boundSql;
    private final TypeHandlerRegistry typeHandlerRegistry;
    private final ObjectFactory objectFactory;
    private final ReflectorFactory reflectorFactory;

    // nested resultmaps
    private final Map<CacheKey, Object> nestedResultObjects = new HashMap<>();
    private final Map<String, Object> ancestorObjects = new HashMap<>();
    private Object previousRowValue;

    // multiple resultsets
    private final Map<String, ResultMapping> nextResultMaps = new HashMap<>();
    private final Map<CacheKey, List<PendingRelation>> pendingRelations = new HashMap<>();

    // Cached Automappings
    private final Map<String, List<UnMappedColumnAutoMapping>> autoMappingsCache = new HashMap<>();
    private final Map<String, List<String>> constructorAutoMappingColumns = new HashMap<>();

    // temporary marking flag that indicate using constructor mapping (use field to reduce memory usage)
    private boolean useConstructorMappings;

    private static class PendingRelation {
        public MetaObject metaObject;
        public ResultMapping propertyMapping;
    }

    private static class UnMappedColumnAutoMapping {
        private final String column;
        private final String property;
        private final TypeHandler<?> typeHandler;
        private final boolean primitive;

        public UnMappedColumnAutoMapping(String column, String property, TypeHandler<?> typeHandler, boolean primitive) {
            this.column = column;
            this.property = property;
            this.typeHandler = typeHandler;
            this.primitive = primitive;
        }
    }

    public DefaultResultSetHandler(Executor executor, MappedStatement mappedStatement, ParameterHandler parameterHandler,
                                   ResultHandler<?> resultHandler, BoundSql boundSql, RowBounds rowBounds) {
        this.executor = executor;
        this.configuration = mappedStatement.getConfiguration();
        this.mappedStatement = mappedStatement;
        this.rowBounds = rowBounds;
        this.parameterHandler = parameterHandler;
        this.boundSql = boundSql;
        this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
        this.objectFactory = configuration.getObjectFactory();
        this.reflectorFactory = configuration.getReflectorFactory();
        this.resultHandler = resultHandler;
    }

    //
    // HANDLE OUTPUT PARAMETER
    //

    @Override
    public void handleOutputParameters(CallableStatement cs) throws SQLException {
        final Object parameterObject = parameterHandler.getParameterObject();
        final MetaObject metaParam = configuration.newMetaObject(parameterObject);
        final List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        for (int i = 0; i < parameterMappings.size(); i++) {
            final ParameterMapping parameterMapping = parameterMappings.get(i);
            if (parameterMapping.getMode() == ParameterMode.OUT || parameterMapping.getMode() == ParameterMode.INOUT) {
                if (ResultSet.class.equals(parameterMapping.getJavaType())) {
                    handleRefCursorOutputParameter((ResultSet) cs.getObject(i + 1), parameterMapping, metaParam);
                } else {
                    final TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
                    metaParam.setValue(parameterMapping.getProperty(), typeHandler.getResult(cs, i + 1));
                }
            }
        }
    }

    private void handleRefCursorOutputParameter(ResultSet rs, ParameterMapping parameterMapping, MetaObject metaParam)
            throws SQLException {
        if (rs == null) {
            return;
        }
        try {
            final String resultMapId = parameterMapping.getResultMapId();
            final ResultMap resultMap = configuration.getResultMap(resultMapId);
            final ResultSetWrapper rsw = new ResultSetWrapper(rs, configuration);
            if (this.resultHandler == null) {
                final DefaultResultHandler resultHandler = new DefaultResultHandler(objectFactory);
                handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
                metaParam.setValue(parameterMapping.getProperty(), resultHandler.getResultList());
            } else {
                handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
            }
        } finally {
            // issue #228 (close resultsets)
            closeResultSet(rs);
        }
    }

    //
    // HANDLE RESULT SETS
    //

    /**
     * 处理结果集
     *
     * @param stmt
     * @return
     * @throws SQLException
     */
    @Override
    public List<Object> handleResultSets(Statement stmt) throws SQLException {
        ErrorContext.instance().activity("handling results").object(mappedStatement.getId());

        // 用于存放多个结果集的数据，元素即为 List<Object>，每一个元素都是一个结果集的结果
        final List<Object> multipleResults = new ArrayList<>();
        // ResultSet 数据数
        int resultSetCount = 0;
        // 获取第一个结果集并进行包装
        ResultSetWrapper rsw = getFirstResultSet(stmt);

        // 从 MappedStatement 中获取 ResultMap 集（可能存在多个结果集）
        List<ResultMap> resultMaps = mappedStatement.getResultMaps();
        int resultMapCount = resultMaps.size();
        // 校验结果集ResultMap长度是否大于0
        validateResultMapsCount(rsw, resultMapCount);

        // 开始处理 ResultMap，直到 resultSetCount 大于 resultMap 时，即设置了多个 resultSets，但设置了较少的 resultMap
        // 这种情况表示着部分 resultSet 可能是被 resultMap 中使用的，如 collection、association 的 resultSet 属性
        while (rsw != null && resultMapCount > resultSetCount) {
            // 按当前结果集的位置来获取对应描述的 ResultMap，因为在 select 中使用 resultSets 时，可以指定多个 resultMap，当然也可以指定一个
            ResultMap resultMap = resultMaps.get(resultSetCount);
            // ****** 处理结果集
            handleResultSet(rsw, resultMap, multipleResults, null);
            // 获取下一个 ResultSet
            rsw = getNextResultSet(stmt);
            // 处理结果集后进行清理
            cleanUpAfterHandlingResultSet();
            resultSetCount++;
        }

        // 当指定了 resultMap 的 resultSet 结果集已经解析完成后（select中 resultMap 与 resultSet 一一对应的情况），
        // 开始解析可能被 ResultMapping 中使用 resultSet 引用的 resultSet（collection、association中 resultSet 引用的情况）
        String[] resultSets = mappedStatement.getResultSets();
        if (resultSets != null) {
            while (rsw != null && resultSetCount < resultSets.length) {
                // 获取引用 resultSet 的 ResultMapping
                ResultMapping parentMapping = nextResultMaps.get(resultSets[resultSetCount]);
                if (parentMapping != null) {
                    // 获取该 ResultMapping 嵌套引用的 ResultMap（<collection resultMap="User" resultSet="user" />）
                    String nestedResultMapId = parentMapping.getNestedResultMapId();
                    ResultMap resultMap = configuration.getResultMap(nestedResultMapId);
                    // 最后通过引用的 ResultMap 来解析处理结果集
                    handleResultSet(rsw, resultMap, null, parentMapping);
                }
                rsw = getNextResultSet(stmt);
                cleanUpAfterHandlingResultSet();
                resultSetCount++;
            }
        }

        return collapseSingleResultList(multipleResults);
    }

    @Override
    public <E> Cursor<E> handleCursorResultSets(Statement stmt) throws SQLException {
        ErrorContext.instance().activity("handling cursor results").object(mappedStatement.getId());
        ResultSetWrapper rsw = getFirstResultSet(stmt);

        List<ResultMap> resultMaps = mappedStatement.getResultMaps();

        int resultMapCount = resultMaps.size();
        validateResultMapsCount(rsw, resultMapCount);
        if (resultMapCount != 1) {
            throw new ExecutorException("Cursor results cannot be mapped to multiple resultMaps");
        }

        ResultMap resultMap = resultMaps.get(0);
        return new DefaultCursor<>(this, resultMap, rsw, rowBounds);
    }

    /**
     * 获取第一个结果集
     * 由于 SQL Driver 支持一次查询多结果集，因此这里可以仅获取第一个结果集
     * @param stmt
     * @return
     * @throws SQLException
     */
    private ResultSetWrapper getFirstResultSet(Statement stmt) throws SQLException {
        ResultSet rs = stmt.getResultSet();
        while (rs == null) {
            // move forward to get the first resultset in case the driver
            // doesn't return the resultset as the first result (HSQLDB)
            if (stmt.getMoreResults()) {
                rs = stmt.getResultSet();
            } else if (stmt.getUpdateCount() == -1) {
                // no more results. Must be no resultset
                break;
            }
        }
        return rs != null ? new ResultSetWrapper(rs, configuration) : null;
    }

    private ResultSetWrapper getNextResultSet(Statement stmt) {
        // Making this method tolerant of bad JDBC drivers
        try {
            if (stmt.getConnection().getMetaData().supportsMultipleResultSets()) {
                // Crazy Standard JDBC way of determining if there are more results
                // DO NOT try to 'improve' the condition even if IDE tells you to!
                // It's important that getUpdateCount() is called here.
                if (!(!stmt.getMoreResults() && stmt.getUpdateCount() == -1)) {
                    ResultSet rs = stmt.getResultSet();
                    if (rs == null) {
                        return getNextResultSet(stmt);
                    } else {
                        return new ResultSetWrapper(rs, configuration);
                    }
                }
            }
        } catch (Exception e) {
            // Intentionally ignored.
        }
        return null;
    }

    private void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    private void cleanUpAfterHandlingResultSet() {
        nestedResultObjects.clear();
    }

    private void validateResultMapsCount(ResultSetWrapper rsw, int resultMapCount) {
        if (rsw != null && resultMapCount < 1) {
            throw new ExecutorException(
                    "A query was run and no Result Maps were found for the Mapped Statement '" + mappedStatement.getId()
                            + "'. 'resultType' or 'resultMap' must be specified when there is no corresponding method.");
        }
    }

    /**
     * 处理结果集
     * @param rsw
     * @param resultMap
     * @param multipleResults
     * @param parentMapping
     * @throws SQLException
     */
    private void handleResultSet(ResultSetWrapper rsw, ResultMap resultMap, List<Object> multipleResults,
                                 ResultMapping parentMapping) throws SQLException {
        try {
            if (parentMapping != null) { // 如果存在父 ResultMapping
                handleRowValues(rsw, resultMap, null, RowBounds.DEFAULT, parentMapping);
            } else if (resultHandler == null) { // 如果未指定 ResultHandler 则使用默认 ResultHandler 来处理
                DefaultResultHandler defaultResultHandler = new DefaultResultHandler(objectFactory);
                handleRowValues(rsw, resultMap, defaultResultHandler, rowBounds, null);
                // DefaultResultHandler 处理完成后，将结果保存到最终的容器中
                multipleResults.add(defaultResultHandler.getResultList());
            } else { // 否则使用指定 ResultHandler 处理行数据
                handleRowValues(rsw, resultMap, resultHandler, rowBounds, null);
            }
        } finally {
            // issue #228 (close resultsets)
            closeResultSet(rsw.getResultSet());
        }
    }

    @SuppressWarnings("unchecked")
    /**
     * 展开结果集，如果只有一个元素，说明只有一个结果集，这时候直接获取该结果集，如果有多个元素说明存在多结果集，直接返回
     */
    private List<Object> collapseSingleResultList(List<Object> multipleResults) {
        return multipleResults.size() == 1 ? (List<Object>) multipleResults.get(0) : multipleResults;
    }

    //
    // HANDLE ROWS FOR SIMPLE RESULTMAP
    //

    /**
     * 处理每行的结果数据
     * @param rsw
     * @param resultMap
     * @param resultHandler
     * @param rowBounds
     * @param parentMapping
     * @throws SQLException
     */
    public void handleRowValues(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler,
                                RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
        if (resultMap.hasNestedResultMaps()) { // 如果存在嵌套ResultMap
            ensureNoRowBounds();
            checkResultHandler();
            handleRowValuesForNestedResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);
        } else {
            handleRowValuesForSimpleResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);
        }
    }

    private void ensureNoRowBounds() {
        if (configuration.isSafeRowBoundsEnabled() && rowBounds != null
                && (rowBounds.getLimit() < RowBounds.NO_ROW_LIMIT || rowBounds.getOffset() > RowBounds.NO_ROW_OFFSET)) {
            throw new ExecutorException(
                    "Mapped Statements with nested result mappings cannot be safely constrained by RowBounds. "
                            + "Use safeRowBoundsEnabled=false setting to bypass this check.");
        }
    }

    protected void checkResultHandler() {
        if (resultHandler != null && configuration.isSafeResultHandlerEnabled() && !mappedStatement.isResultOrdered()) {
            throw new ExecutorException(
                    "Mapped Statements with nested result mappings cannot be safely used with a custom ResultHandler. "
                            + "Use safeResultHandlerEnabled=false setting to bypass this check "
                            + "or ensure your statement returns ordered data and set resultOrdered=true on it.");
        }
    }

    /**
     * 处理普通的 ResultMap
     * @param rsw
     * @param resultMap
     * @param resultHandler
     * @param rowBounds
     * @param parentMapping
     * @throws SQLException
     */
    private void handleRowValuesForSimpleResultMap(ResultSetWrapper rsw, ResultMap resultMap,
                                                   ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
        // 结果上下文
        DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
        ResultSet resultSet = rsw.getResultSet();
        // 通过指定行
        skipRows(resultSet, rowBounds);
        // 如果需要继续处理、ResultSet未关闭并且存在下一行
        while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {
            // 预先按 ResultMap 的 discriminator 来获取实际 ResultMap 对象
            ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);
            // 处理行值
            Object rowValue = getRowValue(rsw, discriminatedResultMap, null);
            // 将结果进行存储，该方法会判断 ResultMapping 是否设置 resultSet 属性，如果设置则遍历 pendingRelations 中需要链接的父对象来设置值
            storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
        }
    }

    /**
     * 存储对象
     * @param resultHandler
     * @param resultContext
     * @param rowValue
     * @param parentMapping
     * @param rs
     * @throws SQLException
     */
    private void storeObject(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue,
                             ResultMapping parentMapping, ResultSet rs) throws SQLException {
        if (parentMapping != null) {
            // 如果 父ResultMapping 不为空，则说明要将该值设置到父对象中
            linkToParents(rs, parentMapping, rowValue);
        } else {
            // 如果不存在 父ResultMapping 时，说明该值是顶层已经完全处理好，直接通过 ResultHandler 来存储值
            callResultHandler(resultHandler, resultContext, rowValue);
        }
    }

    @SuppressWarnings("unchecked" /* because ResultHandler<?> is always ResultHandler<Object> */)
    /**
     * 通过 ResultHandler来存储结果
     */
    private void callResultHandler(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext,
                                   Object rowValue) {
        resultContext.nextResultObject(rowValue); // 记录当前对象到 ResultContext中，并将结果集数量++
        // 使用 ResultContext 来存储对象
        ((ResultHandler<Object>) resultHandler).handleResult(resultContext);
    }

    /***
     * 是否需要继续处理行
     * @param context
     * @param rowBounds
     * @return
     */
    private boolean shouldProcessMoreRows(ResultContext<?> context, RowBounds rowBounds) {
        return !context.isStopped() && context.getResultCount() < rowBounds.getLimit();
    }

    /**
     * 根据 RowBounds 跳过指定行
     * @param rs
     * @param rowBounds
     * @throws SQLException
     */
    private void skipRows(ResultSet rs, RowBounds rowBounds) throws SQLException {
        if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
            if (rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
                rs.absolute(rowBounds.getOffset());
            }
        } else {
            for (int i = 0; i < rowBounds.getOffset(); i++) {
                if (!rs.next()) {
                    break;
                }
            }
        }
    }

    //
    // GET VALUE FROM ROW FOR SIMPLE RESULT MAP
    //

    /**
     * 获取行数据
     * @param rsw
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix) throws SQLException {
        // 结果中延迟加载的属性 map
        final ResultLoaderMap lazyLoader = new ResultLoaderMap();

        Object rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);
        // 判断当前对象是否是使用 TypeHandler 创建的，如果是则无需进行属性映射
        if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
            // 构建一个元信息对象
            final MetaObject metaObject = configuration.newMetaObject(rowValue);
            boolean foundValues = this.useConstructorMappings;
            // 如果需要应用自动填充，则进行自动映射的属性值填充
            if (shouldApplyAutomaticMappings(resultMap, false)) {
                foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
            }
            // 对结果进行显示映射的属性填充，
            foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;
            // 由于可能存在延迟加载，这种情况直接当作找到值处理
            foundValues = lazyLoader.size() > 0 || foundValues;
            // 如果未找到值，或者配置了 returnInstanceForEmptyRow=true 时，则直接返回个空对象，否则直接返回null
            rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
        }
        return rowValue;
    }

    //
    // GET VALUE FROM ROW FOR NESTED RESULT MAP
    //

    /**
     * 获取行数据，
     * 如果行的父对象首次解析，那么这里会将解析的对象，放到 nestedResultObjects 对象中缓存起来
     * 如果已经存在父对象，表明当前行需要找到对应属性进行填充或追加
     * 如：User中属性address是个集合，在解析时
     * 第一种情况就是初始解析，需要把 user 对象也解析出来，
     * 第二种情况，则是 user 对象已解析，在这里只要解析其 address 属性，并放入对应的user.address中即可
     *
     * @param rsw
     * @param resultMap
     * @param combinedKey
     * @param columnPrefix
     * @param partialObject
     * @return
     * @throws SQLException
     */
    private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, CacheKey combinedKey, String columnPrefix,
                               Object partialObject) throws SQLException {
        final String resultMapId = resultMap.getId();
        Object rowValue = partialObject;
        if (rowValue != null) { // 对应第二种情况，父对象已存在
            final MetaObject metaObject = configuration.newMetaObject(rowValue);
            // 储存 resultMapId 与父对象的映射关系
            putAncestor(rowValue, resultMapId);
            // 应用嵌套的结果集映射
            applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, false);
            // 移除原映射关系
            ancestorObjects.remove(resultMapId);
        } else {
            // 创建对象
            final ResultLoaderMap lazyLoader = new ResultLoaderMap();
            rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);
            if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
                final MetaObject metaObject = configuration.newMetaObject(rowValue);
                boolean foundValues = this.useConstructorMappings;
                if (shouldApplyAutomaticMappings(resultMap, true)) {
                    foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
                }
                // 填充非嵌套的参数值
                foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;

                // 同上面的if处理
                putAncestor(rowValue, resultMapId);
                foundValues = applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, true)
                        || foundValues;
                ancestorObjects.remove(resultMapId);

                // 如果未映射到任何值，则进行空值处理
                foundValues = lazyLoader.size() > 0 || foundValues;
                rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
            }
            if (combinedKey != CacheKey.NULL_CACHE_KEY) {
                nestedResultObjects.put(combinedKey, rowValue);
            }
        }
        return rowValue;
    }

    private void putAncestor(Object resultObject, String resultMapId) {
        ancestorObjects.put(resultMapId, resultObject);
    }

    /**
     * 是否支持自动映射，如果需要则进行自动映射
     * @param resultMap
     * @param isNested
     * @return
     */
    private boolean shouldApplyAutomaticMappings(ResultMap resultMap, boolean isNested) {
        if (resultMap.getAutoMapping() != null) {
            return resultMap.getAutoMapping();
        }
        if (isNested) {
            return AutoMappingBehavior.FULL == configuration.getAutoMappingBehavior();
        } else {
            return AutoMappingBehavior.NONE != configuration.getAutoMappingBehavior();
        }
    }

    //
    // PROPERTY MAPPINGS
    //

    /**
     * 进行显示映射的属性填充，该方法不会填充嵌套ResultMap参数
     * @param rsw
     * @param resultMap
     * @param metaObject
     * @param lazyLoader
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private boolean applyPropertyMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject,
                                          ResultLoaderMap lazyLoader, String columnPrefix) throws SQLException {
        final Set<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);
        boolean foundValues = false;
        final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();
        // 遍历所有映射集进行填充值
        for (ResultMapping propertyMapping : propertyMappings) {
            String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
            // 判断是否有嵌套映射，如果有则无需使用column来处理值
            if (propertyMapping.getNestedResultMapId() != null) {
                // the user added a column attribute to a nested result map, ignore it
                column = null;
            }
            if (propertyMapping.isCompositeResult() // 是否是嵌套查询
                    || column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH)) // 是否column不为空，并且已知映射的字段名字包含该column
                    || propertyMapping.getResultSet() != null) { //
                // 获取对应属性值
                Object value = getPropertyMappingValue(rsw.getResultSet(), metaObject, propertyMapping, lazyLoader,
                        columnPrefix);
                // issue #541 make property optional
                final String property = propertyMapping.getProperty();
                if (property == null) {
                    continue;
                }
                // value 如果是 DEFERRED，即 延期加载，则跳过处理，常见延期的有几种情况：
                // 1使用了延迟加载；
                // 2查询操作已执行即缓存中存在，但无法确认已经执行完成，还是仍在执行中
                if (value == DEFERRED) {
                    foundValues = true;
                    continue;
                }
                // 如果找到具体值
                if (value != null) {
                    foundValues = true;
                }
                // 值非空，或者允许值为null时调用set方法，并且该set方法非基本类型的入参时，则进行调用
                if (value != null
                        || configuration.isCallSettersOnNulls() && !metaObject.getSetterType(property).isPrimitive()) {
                    // gcode issue #377, call setter on nulls (value is not 'found')
                    metaObject.setValue(property, value);
                }
            }
        }
        return foundValues;
    }

    /**
     * 获取属性值
     * @param rs
     * @param metaResultObject
     * @param propertyMapping
     * @param lazyLoader
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object getPropertyMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping,
                                           ResultLoaderMap lazyLoader, String columnPrefix) throws SQLException {
        if (propertyMapping.getNestedQueryId() != null) {
            // 如果是嵌套查询属性，则处理嵌套查询操作
            return getNestedQueryMappingValue(rs, metaResultObject, propertyMapping, lazyLoader, columnPrefix);
        }
        // 获取值时，如果当前 ResultMapping 指定了 resultSet 属性，
        // 则记录当前 ResultMapping ，表示该 ResultMapping 引用的值是通过其他 ResultSet 获取的
        // 因此，在其他 ResultSet 解析完成后，在存储值时，会回来看引用该 ResultSet 结果的 RequestMapping，找到后将会把值进行赋值
        // 可看 org.apache.ibatis.executor.resultset.DefaultResultSetHandler.linkToParents，在 storeObject 方法中被使用
        if (propertyMapping.getResultSet() != null) {
            addPendingChildRelation(rs, metaResultObject, propertyMapping);
            return DEFERRED;
        } else {
            // 其他方式（普通）的则直接通过 TypeHandler 进行解析值即可
            final TypeHandler<?> typeHandler = propertyMapping.getTypeHandler();
            final String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
            return typeHandler.getResult(rs, column);
        }
    }

    /**
     * 创建自动映射列表
     * @param rsw
     * @param resultMap
     * @param metaObject
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private List<UnMappedColumnAutoMapping> createAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap,
                                                                    MetaObject metaObject, String columnPrefix) throws SQLException {
        final String mapKey = resultMap.getId() + ":" + columnPrefix;
        List<UnMappedColumnAutoMapping> autoMapping = autoMappingsCache.get(mapKey);
        if (autoMapping == null) {
            autoMapping = new ArrayList<>();
            // 获取所有未显示声明映射规则的字段名称
            final List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);
            List<String> mappedInConstructorAutoMapping = constructorAutoMappingColumns.remove(mapKey);
            if (mappedInConstructorAutoMapping != null) {
                unmappedColumnNames.removeAll(mappedInConstructorAutoMapping);
            }
            // 迭代生成自动映射
            for (String columnName : unmappedColumnNames) {
                String propertyName = columnName;
                if (columnPrefix != null && !columnPrefix.isEmpty()) {
                    // When columnPrefix is specified,
                    // ignore columns without the prefix.
                    if (!columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
                        continue;
                    }
                    propertyName = columnName.substring(columnPrefix.length());
                }
                // 查找指定属性
                final String property = metaObject.findProperty(propertyName, configuration.isMapUnderscoreToCamelCase());
                // 如果属性存在并且有set方法，则将其添加到 自动映射集合中
                if (property != null && metaObject.hasSetter(property)) {
                    if (resultMap.getMappedProperties().contains(property)) {
                        continue;
                    }
                    final Class<?> propertyType = metaObject.getSetterType(property);
                    if (typeHandlerRegistry.hasTypeHandler(propertyType, rsw.getJdbcType(columnName))) {
                        final TypeHandler<?> typeHandler = rsw.getTypeHandler(propertyType, columnName);
                        autoMapping
                                .add(new UnMappedColumnAutoMapping(columnName, property, typeHandler, propertyType.isPrimitive()));
                    } else { // 如果不存在对应的 TypeHandler 则根据 未映射字段行为 来处理
                        configuration.getAutoMappingUnknownColumnBehavior().doAction(mappedStatement, columnName, property,
                                propertyType);
                    }
                } else {
                    configuration.getAutoMappingUnknownColumnBehavior().doAction(mappedStatement, columnName,
                            property != null ? property : propertyName, null);
                }
            }
            autoMappingsCache.put(mapKey, autoMapping);
        }
        return autoMapping;
    }

    /**
     * 基于自动映射自动找到对应字段的值
     * @param rsw
     * @param resultMap
     * @param metaObject
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private boolean applyAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject,
                                           String columnPrefix) throws SQLException {
        // 构建一个自动映射对象列表
        List<UnMappedColumnAutoMapping> autoMapping = createAutomaticMappings(rsw, resultMap, metaObject, columnPrefix);
        boolean foundValues = false;
        if (!autoMapping.isEmpty()) {
            // 遍历自动填充列表，并进行属性值的填充
            for (UnMappedColumnAutoMapping mapping : autoMapping) {
                // 根据自动填充的mapping获取值
                final Object value = mapping.typeHandler.getResult(rsw.getResultSet(), mapping.column);
                if (value != null) {
                    foundValues = true;
                }
                // 进行值的设置
                if (value != null || configuration.isCallSettersOnNulls() && !mapping.primitive) {
                    // gcode issue #377, call setter on nulls (value is not 'found')
                    metaObject.setValue(mapping.property, value);
                }
            }
        }
        return foundValues;
    }

    // MULTIPLE RESULT SETS

    /**
     * 将值与父对象进行关联
     * 该方法会遍历 pendingRelations ，也就是在 org.apache.ibatis.executor.resultset.DefaultResultSetHandler#addPendingChildRelation(java.sql.ResultSet, org.apache.ibatis.reflection.MetaObject, org.apache.ibatis.mapping.ResultMapping)
     * 方法中所添加的以来 resultSet 的 ResultMapping 引用。该方法会将 resultSet 解析到的值，按 ResultMapping 重新填入 pendingRelations 关联的对象中去
     * @param rs
     * @param parentMapping
     * @param rowValue
     * @throws SQLException
     */
    private void linkToParents(ResultSet rs, ResultMapping parentMapping, Object rowValue) throws SQLException {
        CacheKey parentKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(),
                parentMapping.getForeignColumn());
        // 获取其遍历所有父类对象，由于可能多个父类使用同一个结果集，所以这里会将值链接到获取到的父类对象中
        List<PendingRelation> parents = pendingRelations.get(parentKey);
        if (parents != null) {
            for (PendingRelation parent : parents) {
                if (parent != null && rowValue != null) {
                    linkObjects(parent.metaObject, parent.propertyMapping, rowValue);
                }
            }
        }
    }

    /**
     * 添加待解析的子关联，在 ResultMapping 指定了 resultSet 时，表明该 ResultMapping 对应 属性 的值要从指定名称的 resultSet 获取
     * 因此该方法将会记录 ResultMapping，待对应的 resultSet 解析时，将会遍历引用对应 resultSet 的 ResultMapping，进行赋值操作
     * 可看 org.apache.ibatis.executor.resultset.DefaultResultSetHandler.linkToParents，在 storeObject 方法中被使用
     * @param rs
     * @param metaResultObject
     * @param parentMapping
     * @throws SQLException
     */
    private void addPendingChildRelation(ResultSet rs, MetaObject metaResultObject, ResultMapping parentMapping)
            throws SQLException {
        // 创建对应的 key
        CacheKey cacheKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(),
                parentMapping.getColumn());
        // 添加预处理关联对象，记录当前 元对象，以及当前的 RequestMapping 对象
        PendingRelation deferLoad = new PendingRelation();
        deferLoad.metaObject = metaResultObject;
        deferLoad.propertyMapping = parentMapping;
        // 获取关联列表并添加
        List<PendingRelation> relations = MapUtil.computeIfAbsent(pendingRelations, cacheKey, k -> new ArrayList<>());
        relations.add(deferLoad);
        // 获取需要引用 resultSet 的 ResultMapping，防止存在多个 ResultMapping（即属性） 引用
        ResultMapping previous = nextResultMaps.get(parentMapping.getResultSet());
        if (previous == null) {
            nextResultMaps.put(parentMapping.getResultSet(), parentMapping);
        } else if (!previous.equals(parentMapping)) {
            throw new ExecutorException("Two different properties are mapped to the same resultSet");
        }
    }

    private CacheKey createKeyForMultipleResults(ResultSet rs, ResultMapping resultMapping, String names, String columns)
            throws SQLException {
        CacheKey cacheKey = new CacheKey();
        cacheKey.update(resultMapping);
        if (columns != null && names != null) {
            String[] columnsArray = columns.split(",");
            String[] namesArray = names.split(",");
            for (int i = 0; i < columnsArray.length; i++) {
                Object value = rs.getString(columnsArray[i]);
                if (value != null) {
                    cacheKey.update(namesArray[i]);
                    cacheKey.update(value);
                }
            }
        }
        return cacheKey;
    }

    //
    // INSTANTIATION & CONSTRUCTOR MAPPING
    //

    /**
     * 创建存放结果的对象
     * @param rsw
     * @param resultMap
     * @param lazyLoader
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, ResultLoaderMap lazyLoader,
                                      String columnPrefix) throws SQLException {
        this.useConstructorMappings = false; // reset previous mapping result
        final List<Class<?>> constructorArgTypes = new ArrayList<>();
        final List<Object> constructorArgs = new ArrayList<>();
        // 创建结果对象
        Object resultObject = createResultObject(rsw, resultMap, constructorArgTypes, constructorArgs, columnPrefix);
        // 判断当前对象是否是使用 TypeHandler 创建的，如果是则无需进行属性映射
        if (resultObject != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
            // 判断映射参数是否存在 嵌套sql，并且
            final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();
            for (ResultMapping propertyMapping : propertyMappings) {
                // 如果是嵌套查询，并且是懒加载，则创建一个懒加载代理对象，而不是在这里进行实际的参数映射处理
                // 注意，如果存在多个属性是懒加载时，mybatis会不断给该对象进行代理（类似装饰者），不断套娃
                if (propertyMapping.getNestedQueryId() != null && propertyMapping.isLazy()) {
                    resultObject = configuration.getProxyFactory().createProxy(resultObject, lazyLoader, configuration,
                            objectFactory, constructorArgTypes, constructorArgs);
                    break;
                }
            }
        }
        this.useConstructorMappings = resultObject != null && !constructorArgTypes.isEmpty(); // set current mapping result
        return resultObject;
    }

    /**
     *
     * @param rsw
     * @param resultMap
     * @param constructorArgTypes
     * @param constructorArgs
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, List<Class<?>> constructorArgTypes,
                                      List<Object> constructorArgs, String columnPrefix) throws SQLException {
        final Class<?> resultType = resultMap.getType(); // 获取 ResultMap 的类型
        final MetaClass metaType = MetaClass.forClass(resultType, reflectorFactory);
        // 获取构造器映射
        final List<ResultMapping> constructorMappings = resultMap.getConstructorResultMappings();
        // 判断是否存在resultMap 的类型处理器，如果存在则通过类型处理器的方式来创建结果
        if (hasTypeHandlerForResultObject(rsw, resultType)) {
            return createPrimitiveResultObject(rsw, resultMap, columnPrefix);
        }
        // 如果不存在类型处理器，则判断是否常用构造器方式构建对象
        if (!constructorMappings.isEmpty()) {
            return createParameterizedResultObject(rsw, resultType, constructorMappings, constructorArgTypes, constructorArgs,
                    columnPrefix);
        } else if (resultType.isInterface() || metaType.hasDefaultConstructor()) {
            // 如果目标类型是接口，或则存在默认构造器，则直接通过 ObjectFactory 工厂构建
            return objectFactory.create(resultType);
        } else if (shouldApplyAutomaticMappings(resultMap, false)) {
            // 如果非接口，并且无无参构造器则直接使用自动映射构建对象
            return createByConstructorSignature(rsw, resultMap, columnPrefix, resultType, constructorArgTypes,
                    constructorArgs);
        }
        throw new ExecutorException("Do not know how to create an instance of " + resultType);
    }

    /**
     * 创建参数化结果对象
     * @param rsw
     * @param resultType
     * @param constructorMappings
     * @param constructorArgTypes
     * @param constructorArgs
     * @param columnPrefix
     * @return
     */
    Object createParameterizedResultObject(ResultSetWrapper rsw, Class<?> resultType,
                                           List<ResultMapping> constructorMappings, List<Class<?>> constructorArgTypes, List<Object> constructorArgs,
                                           String columnPrefix) {
        boolean foundValues = false; // 是否找到值
        for (ResultMapping constructorMapping : constructorMappings) {
            // 获取构造映射的参数类型、字段名
            final Class<?> parameterType = constructorMapping.getJavaType();
            final String column = constructorMapping.getColumn();
            final Object value;
            try {
                if (constructorMapping.getNestedQueryId() != null) {
                    // 判断构造参数mapping是否有 嵌套查询id，则通过嵌套查询获取构造器参数值
                    value = getNestedQueryConstructorValue(rsw.getResultSet(), constructorMapping, columnPrefix);
                } else if (constructorMapping.getNestedResultMapId() != null) {
                    // 如果是嵌套 ResultMap 则基于当前行数据和嵌套map来构建对象
                    String constructorColumnPrefix = getColumnPrefix(columnPrefix, constructorMapping);
                    final ResultMap resultMap = resolveDiscriminatedResultMap(rsw.getResultSet(),
                            configuration.getResultMap(constructorMapping.getNestedResultMapId()), constructorColumnPrefix);
                    value = getRowValue(rsw, resultMap, constructorColumnPrefix);
                } else {
                    // 否则直接获取映射的类型处理器，来获取对应字段的值
                    final TypeHandler<?> typeHandler = constructorMapping.getTypeHandler();
                    value = typeHandler.getResult(rsw.getResultSet(), prependPrefix(column, columnPrefix));
                }
            } catch (ResultMapException | SQLException e) {
                throw new ExecutorException("Could not process result for mapping: " + constructorMapping, e);
            }
            // 往构造器参数类型和参数值中添加处理后的结果
            constructorArgTypes.add(parameterType);
            constructorArgs.add(value);
            // 如果值不为空说明找到对应的值
            foundValues = value != null || foundValues;
        }
        // 如果找到值则说明可以进行构建对象，否则返回null
        return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
    }

    private Object createByConstructorSignature(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix,
                                                Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs) throws SQLException {
        return applyConstructorAutomapping(rsw, resultMap, columnPrefix, resultType, constructorArgTypes, constructorArgs,
                findConstructorForAutomapping(resultType, rsw).orElseThrow(() -> new ExecutorException(
                        "No constructor found in " + resultType.getName() + " matching " + rsw.getClassNames())));
    }

    private Optional<Constructor<?>> findConstructorForAutomapping(final Class<?> resultType, ResultSetWrapper rsw) {
        Constructor<?>[] constructors = resultType.getDeclaredConstructors();
        if (constructors.length == 1) {
            return Optional.of(constructors[0]);
        }
        Optional<Constructor<?>> annotated = Arrays.stream(constructors)
                .filter(x -> x.isAnnotationPresent(AutomapConstructor.class)).reduce((x, y) -> {
                    throw new ExecutorException("@AutomapConstructor should be used in only one constructor.");
                });
        if (annotated.isPresent()) {
            return annotated;
        }
        if (configuration.isArgNameBasedConstructorAutoMapping()) {
            // Finding-best-match type implementation is possible,
            // but using @AutomapConstructor seems sufficient.
            throw new ExecutorException(MessageFormat.format(
                    "'argNameBasedConstructorAutoMapping' is enabled and the class ''{0}'' has multiple constructors, so @AutomapConstructor must be added to one of the constructors.",
                    resultType.getName()));
        } else {
            return Arrays.stream(constructors).filter(x -> findUsableConstructorByArgTypes(x, rsw.getJdbcTypes())).findAny();
        }
    }

    private boolean findUsableConstructorByArgTypes(final Constructor<?> constructor, final List<JdbcType> jdbcTypes) {
        final Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length != jdbcTypes.size()) {
            return false;
        }
        for (int i = 0; i < parameterTypes.length; i++) {
            if (!typeHandlerRegistry.hasTypeHandler(parameterTypes[i], jdbcTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Object applyConstructorAutomapping(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix,
                                               Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, Constructor<?> constructor)
            throws SQLException {
        boolean foundValues = false;
        if (configuration.isArgNameBasedConstructorAutoMapping()) {
            foundValues = applyArgNameBasedConstructorAutoMapping(rsw, resultMap, columnPrefix, constructorArgTypes,
                    constructorArgs, constructor, foundValues);
        } else {
            foundValues = applyColumnOrderBasedConstructorAutomapping(rsw, constructorArgTypes, constructorArgs, constructor,
                    foundValues);
        }
        return foundValues || configuration.isReturnInstanceForEmptyRow()
                ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
    }

    private boolean applyColumnOrderBasedConstructorAutomapping(ResultSetWrapper rsw, List<Class<?>> constructorArgTypes,
                                                                List<Object> constructorArgs, Constructor<?> constructor, boolean foundValues) throws SQLException {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            String columnName = rsw.getColumnNames().get(i);
            TypeHandler<?> typeHandler = rsw.getTypeHandler(parameterType, columnName);
            Object value = typeHandler.getResult(rsw.getResultSet(), columnName);
            constructorArgTypes.add(parameterType);
            constructorArgs.add(value);
            foundValues = value != null || foundValues;
        }
        return foundValues;
    }

    private boolean applyArgNameBasedConstructorAutoMapping(ResultSetWrapper rsw, ResultMap resultMap,
                                                            String columnPrefix, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, Constructor<?> constructor,
                                                            boolean foundValues) throws SQLException {
        List<String> missingArgs = null;
        Parameter[] params = constructor.getParameters();
        for (Parameter param : params) {
            boolean columnNotFound = true;
            Param paramAnno = param.getAnnotation(Param.class);
            String paramName = paramAnno == null ? param.getName() : paramAnno.value();
            for (String columnName : rsw.getColumnNames()) {
                if (columnMatchesParam(columnName, paramName, columnPrefix)) {
                    Class<?> paramType = param.getType();
                    TypeHandler<?> typeHandler = rsw.getTypeHandler(paramType, columnName);
                    Object value = typeHandler.getResult(rsw.getResultSet(), columnName);
                    constructorArgTypes.add(paramType);
                    constructorArgs.add(value);
                    final String mapKey = resultMap.getId() + ":" + columnPrefix;
                    if (!autoMappingsCache.containsKey(mapKey)) {
                        MapUtil.computeIfAbsent(constructorAutoMappingColumns, mapKey, k -> new ArrayList<>()).add(columnName);
                    }
                    columnNotFound = false;
                    foundValues = value != null || foundValues;
                }
            }
            if (columnNotFound) {
                if (missingArgs == null) {
                    missingArgs = new ArrayList<>();
                }
                missingArgs.add(paramName);
            }
        }
        if (foundValues && constructorArgs.size() < params.length) {
            throw new ExecutorException(MessageFormat.format(
                    "Constructor auto-mapping of ''{1}'' failed " + "because ''{0}'' were not found in the result set; "
                            + "Available columns are ''{2}'' and mapUnderscoreToCamelCase is ''{3}''.",
                    missingArgs, constructor, rsw.getColumnNames(), configuration.isMapUnderscoreToCamelCase()));
        }
        return foundValues;
    }

    private boolean columnMatchesParam(String columnName, String paramName, String columnPrefix) {
        if (columnPrefix != null) {
            if (!columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
                return false;
            }
            columnName = columnName.substring(columnPrefix.length());
        }
        return paramName
                .equalsIgnoreCase(configuration.isMapUnderscoreToCamelCase() ? columnName.replace("_", "") : columnName);
    }

    /**
     *
     * @param rsw
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object createPrimitiveResultObject(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix)
            throws SQLException {
        final Class<?> resultType = resultMap.getType();
        final String columnName;
        if (!resultMap.getResultMappings().isEmpty()) {
            // 如果显示指定映射，使用第一个字段来获取 TypeHandler
            final List<ResultMapping> resultMappingList = resultMap.getResultMappings();
            final ResultMapping mapping = resultMappingList.get(0);
            columnName = prependPrefix(mapping.getColumn(), columnPrefix);
        } else {
            // 否则获取sql的第一个字段名
            columnName = rsw.getColumnNames().get(0);
        }
        // 获取 TypeHandler，注意，由于获取 TypeHandler 时会根据 类型+第一个字段的jdbcType 来获取 TypeHandler
        // 所以如果是注册返回结果的 TypeHandler 时，请注意无须指定 JdbcType
        final TypeHandler<?> typeHandler = rsw.getTypeHandler(resultType, columnName);
        // 通过 TypeHandler 创建 结果对象
        return typeHandler.getResult(rsw.getResultSet(), columnName);
    }

    //
    // NESTED QUERY
    //

    private Object getNestedQueryConstructorValue(ResultSet rs, ResultMapping constructorMapping, String columnPrefix)
            throws SQLException {
        final String nestedQueryId = constructorMapping.getNestedQueryId();
        final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
        final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
        final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, constructorMapping,
                nestedQueryParameterType, columnPrefix);
        Object value = null;
        if (nestedQueryParameterObject != null) {
            final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
            final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT,
                    nestedBoundSql);
            final Class<?> targetType = constructorMapping.getJavaType();
            final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery,
                    nestedQueryParameterObject, targetType, key, nestedBoundSql);
            value = resultLoader.loadResult();
        }
        return value;
    }

    /**
     * 执行嵌套查询，获取嵌套查询的结果值
     * @param rs
     * @param metaResultObject
     * @param propertyMapping
     * @param lazyLoader
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object getNestedQueryMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping,
                                              ResultLoaderMap lazyLoader, String columnPrefix) throws SQLException {
        // 获取嵌套查询相关信息
        final String nestedQueryId = propertyMapping.getNestedQueryId();
        final String property = propertyMapping.getProperty();
        final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
        // 获取嵌套查询statement所需的参数类型
        final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
        // 预处理嵌套查询的参数对象
        final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, propertyMapping,
                nestedQueryParameterType, columnPrefix);
        Object value = null;
        // 如果入参不为空，则表示需要执行嵌套查询操作
        if (nestedQueryParameterObject != null) {
            final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
            final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT,
                    nestedBoundSql);
            final Class<?> targetType = propertyMapping.getJavaType();
            // 如果存在缓存，则将该属性标记为延迟加载
            // 这时候不着急从缓存中获取值，因为可能缓存中的对象并未是最终对象，可能是延迟加载的占位符
            if (executor.isCached(nestedQuery, key)) {
                executor.deferLoad(nestedQuery, metaResultObject, property, key, targetType);
                value = DEFERRED;
            } else {
                // 如果缓存中不存在，则直接加载
                final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery,
                        nestedQueryParameterObject, targetType, key, nestedBoundSql);
                if (propertyMapping.isLazy()) {
                    lazyLoader.addLoader(property, metaResultObject, resultLoader);
                    value = DEFERRED;
                } else {
                    value = resultLoader.loadResult();
                }
            }
        }
        return value;
    }

    /**
     * 预处理嵌套查询的参数
     * @param rs
     * @param resultMapping
     * @param parameterType
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object prepareParameterForNestedQuery(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType,
                                                  String columnPrefix) throws SQLException {
        // 如果是嵌套查询的复合参数，如column使用了表达式：{id=group_id,state=state}，这种情况需要将
        // 需要安装表达式规则将其转为具体对象
        if (resultMapping.isCompositeResult()) {
            return prepareCompositeKeyParameter(rs, resultMapping, parameterType, columnPrefix);
        }
        // 否则就返回简单的参数
        return prepareSimpleKeyParameter(rs, resultMapping, parameterType, columnPrefix);
    }

    /**
     * 预处理嵌套查询简单的参数对象（未使用表达式的）
     * @param rs
     * @param resultMapping
     * @param parameterType
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object prepareSimpleKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType,
                                             String columnPrefix) throws SQLException {
        final TypeHandler<?> typeHandler;
        // 直接获取 TypeHandler 进行处理即可
        if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
            typeHandler = typeHandlerRegistry.getTypeHandler(parameterType);
        } else {
            typeHandler = typeHandlerRegistry.getUnknownTypeHandler();
        }
        return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
    }

    /**
     * 预处理嵌套查询的参数（如column使用复杂表达式 {id=group_id,state=state}）
     * @param rs
     * @param resultMapping
     * @param parameterType
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object prepareCompositeKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType,
                                                String columnPrefix) throws SQLException {
        // 使用嵌套对应statement所需的参数类型来创建实例对象，并构建 MetaObject 对象
        final Object parameterObject = instantiateParameterObject(parameterType);
        final MetaObject metaObject = configuration.newMetaObject(parameterObject);
        boolean foundValues = false;
        // 从 ResultSet 中获取值，并按映射规则进行值的填充
        for (ResultMapping innerResultMapping : resultMapping.getComposites()) {
            // 获取指定属性set方法的参数类型，并获取其类型处理器
            final Class<?> propType = metaObject.getSetterType(innerResultMapping.getProperty());
            final TypeHandler<?> typeHandler = typeHandlerRegistry.getTypeHandler(propType);
            // 使用 TypeHandler 从 ResultSet 中获取值
            final Object propValue = typeHandler.getResult(rs, prependPrefix(innerResultMapping.getColumn(), columnPrefix));
            // issue #353 & #560 do not execute nested query if key is null
            // 如果值为空，则进行属性的设置
            if (propValue != null) {
                metaObject.setValue(innerResultMapping.getProperty(), propValue);
                foundValues = true;
            }
        }
        // 如果任意一个属性不为空，则直接返回最终填充好的对象
        return foundValues ? parameterObject : null;
    }

    /**
     * 构建参数对象，如果 未指定参数类型 或 ParamMap 类型则创建HashMap，否则直接通过 ObjectFactory 构建对象
     * @param parameterType
     * @return
     */
    private Object instantiateParameterObject(Class<?> parameterType) {
        if (parameterType == null) {
            return new HashMap<>();
        }
        if (ParamMap.class.equals(parameterType)) {
            return new HashMap<>(); // issue #649
        } else {
            return objectFactory.create(parameterType);
        }
    }

    //
    // DISCRIMINATOR
    //

    /**
     * 根据 ResultMap 中记录的 Discriminator 对象以及参与映射的记录行中的列值，确定使用的ResultMap对象
     * 如果没 Discriminator 则返回原对象
     * @param rs
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    public ResultMap resolveDiscriminatedResultMap(ResultSet rs, ResultMap resultMap, String columnPrefix)
            throws SQLException {
        Set<String> pastDiscriminators = new HashSet<>();
        Discriminator discriminator = resultMap.getDiscriminator();
        // 不断递归调用，得到最终映射的 ResultMap
        while (discriminator != null) {
            final Object value = getDiscriminatorValue(rs, discriminator, columnPrefix);
            final String discriminatedMapId = discriminator.getMapIdFor(String.valueOf(value));
            if (!configuration.hasResultMap(discriminatedMapId)) {
                break;
            }
            resultMap = configuration.getResultMap(discriminatedMapId);
            Discriminator lastDiscriminator = discriminator;
            discriminator = resultMap.getDiscriminator();
            if (discriminator == lastDiscriminator || !pastDiscriminators.add(discriminatedMapId)) {
                break;
            }
        }
        return resultMap;
    }

    private Object getDiscriminatorValue(ResultSet rs, Discriminator discriminator, String columnPrefix)
            throws SQLException {
        final ResultMapping resultMapping = discriminator.getResultMapping();
        final TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
        return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
    }

    private String prependPrefix(String columnName, String prefix) {
        if (columnName == null || columnName.length() == 0 || prefix == null || prefix.length() == 0) {
            return columnName;
        }
        return prefix + columnName;
    }

    //
    // HANDLE NESTED RESULT MAPS
    //

    /**
     * 将行数据按嵌套ResultMap 解析出最终结果
     * 方法里面关键地方有两个，分别是：
     * 1. storeObject：这个方法之前简单对象解析也看过，就是将解析后的对象，存储到 ResultHandler 中
     * 2. getRowValue：获取解析值，该值需要传入参数：(ResultSetWrapper rsw, ResultMap resultMap, CacheKey combinedKey, String columnPrefix, Object partialObject)
     *                 其中的 partialObject 表示祖先（父）对象，如果对象不为空，则说明解析的值直接嵌套添加到指定属性中即可；
     *                 如果 partialObject 为空则除了加载内嵌对象外，还需要把主对象的属性也加载设置进去
     * @param rsw
     * @param resultMap
     * @param resultHandler
     * @param rowBounds
     * @param parentMapping
     * @throws SQLException
     */
    private void handleRowValuesForNestedResultMap(ResultSetWrapper rsw, ResultMap resultMap,
                                                   ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
        final DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
        ResultSet resultSet = rsw.getResultSet();
        skipRows(resultSet, rowBounds);
        Object rowValue = previousRowValue;
        while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {
            // 获取结果 Discriminator 处理后最终的 ResultMap
            final ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);
            // 由于最终处理 Discriminator 处理后的 ResultMap，是最终的 ResultMap（如 org.apache.ibatis.builder.xml.XMLMapperBuilder.processNestedResultMappings ），内部的association、collection、discriminator都
            // 被摊开，所以在这里 ResultMap 将可以作为每个描述对象。
            // 由于获取缓存key对象时仅基于第一层属性来创建缓存对象，因此基于该对象获取得到的缓存key对象可以作为描述最外层父对象的存储对象
            // 例如 collection 时，仅需填充其属性，这时便可通过 CacheKey 找到最外层对象来处理
            final CacheKey rowKey = createRowKey(discriminatedResultMap, rsw, null);
            Object partialObject = nestedResultObjects.get(rowKey);
            // resultMap 中的配置属性 resultOrdered，如果是 resultOrdered 则每次操作无需存储嵌套父对象，直接执行储存对象操作即可
            // 也就是说当true时则认为返回一个主结果行时，不会记录nestedResultObject
            if (mappedStatement.isResultOrdered()) {
                // partialObject 为空时，则说明主对象发生了变化，这时则要清除缓存并保存上一次解析的结果对象（rowValue）
                if (partialObject == null && rowValue != null) {
                    nestedResultObjects.clear();
                    storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
                }
                rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
            } else {
                rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
                // 非 resultOrder 时， partialObject 为空说明该主对象是第一次解析到，则将其存储
                if (partialObject == null) {
                    storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
                }
            }
        }
        if (rowValue != null && mappedStatement.isResultOrdered() && shouldProcessMoreRows(resultContext, rowBounds)) {
            storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
            previousRowValue = null;
        } else if (rowValue != null) {
            previousRowValue = rowValue;
        }
    }

    //
    // NESTED RESULT MAP (JOIN MAPPING)
    //

    /**
     * 遍历当前 ResultMap 的所有 ResultMapping 并对所有嵌套的属性进行处理
     * @param rsw
     * @param resultMap 父ResultMap
     * @param metaObject
     * @param parentPrefix
     * @param parentRowKey
     * @param newObject
     * @return
     */
    private boolean applyNestedResultMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject,
                                              String parentPrefix, CacheKey parentRowKey, boolean newObject) {
        boolean foundValues = false;
        for (ResultMapping resultMapping : resultMap.getPropertyResultMappings()) {
            final String nestedResultMapId = resultMapping.getNestedResultMapId();
            if (nestedResultMapId != null && resultMapping.getResultSet() == null) {
                try {
                    // 获取属性前缀
                    final String columnPrefix = getColumnPrefix(parentPrefix, resultMapping);
                    // 获取嵌套ResultMap描述信息
                    final ResultMap nestedResultMap = getNestedResultMap(rsw.getResultSet(), nestedResultMapId, columnPrefix);
                    if (resultMapping.getColumnPrefix() == null) {
                        // try to fill circular reference only when columnPrefix
                        // is not specified for the nested result map (issue #215)
                        Object ancestorObject = ancestorObjects.get(nestedResultMapId);
                        if (ancestorObject != null) {
                            if (newObject) {
                                linkObjects(metaObject, resultMapping, ancestorObject); // issue #385
                            }
                            continue;
                        }
                    }
                    final CacheKey rowKey = createRowKey(nestedResultMap, rsw, columnPrefix);
                    final CacheKey combinedKey = combineKeys(rowKey, parentRowKey);
                    Object rowValue = nestedResultObjects.get(combinedKey);
                    boolean knownValue = rowValue != null;
                    instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject); // mandatory
                    if (anyNotNullColumnHasValue(resultMapping, columnPrefix, rsw)) {
                        rowValue = getRowValue(rsw, nestedResultMap, combinedKey, columnPrefix, rowValue);
                        if (rowValue != null && !knownValue) {
                            linkObjects(metaObject, resultMapping, rowValue);
                            foundValues = true;
                        }
                    }
                } catch (SQLException e) {
                    throw new ExecutorException(
                            "Error getting nested result map values for '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
                }
            }
        }
        return foundValues;
    }

    private String getColumnPrefix(String parentPrefix, ResultMapping resultMapping) {
        final StringBuilder columnPrefixBuilder = new StringBuilder();
        if (parentPrefix != null) {
            columnPrefixBuilder.append(parentPrefix);
        }
        if (resultMapping.getColumnPrefix() != null) {
            columnPrefixBuilder.append(resultMapping.getColumnPrefix());
        }
        return columnPrefixBuilder.length() == 0 ? null : columnPrefixBuilder.toString().toUpperCase(Locale.ENGLISH);
    }

    private boolean anyNotNullColumnHasValue(ResultMapping resultMapping, String columnPrefix, ResultSetWrapper rsw)
            throws SQLException {
        Set<String> notNullColumns = resultMapping.getNotNullColumns();
        if (notNullColumns != null && !notNullColumns.isEmpty()) {
            ResultSet rs = rsw.getResultSet();
            for (String column : notNullColumns) {
                rs.getObject(prependPrefix(column, columnPrefix));
                if (!rs.wasNull()) {
                    return true;
                }
            }
            return false;
        }
        if (columnPrefix != null) {
            for (String columnName : rsw.getColumnNames()) {
                if (columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix.toUpperCase(Locale.ENGLISH))) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private ResultMap getNestedResultMap(ResultSet rs, String nestedResultMapId, String columnPrefix)
            throws SQLException {
        ResultMap nestedResultMap = configuration.getResultMap(nestedResultMapId);
        return resolveDiscriminatedResultMap(rs, nestedResultMap, columnPrefix);
    }

    //
    // UNIQUE RESULT KEY
    //

    /**
     * 创建缓存key，该方法只会根据最外层的 ResultMapping 映射关系，来
     * @param resultMap
     * @param rsw
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private CacheKey createRowKey(ResultMap resultMap, ResultSetWrapper rsw, String columnPrefix) throws SQLException {
        final CacheKey cacheKey = new CacheKey();
        cacheKey.update(resultMap.getId());
        List<ResultMapping> resultMappings = getResultMappingsForRowKey(resultMap);
        if (resultMappings.isEmpty()) {
            if (Map.class.isAssignableFrom(resultMap.getType())) {
                createRowKeyForMap(rsw, cacheKey);
            } else {
                createRowKeyForUnmappedProperties(resultMap, rsw, cacheKey, columnPrefix);
            }
        } else {
            createRowKeyForMappedProperties(resultMap, rsw, cacheKey, resultMappings, columnPrefix);
        }
        if (cacheKey.getUpdateCount() < 2) {
            return CacheKey.NULL_CACHE_KEY;
        }
        return cacheKey;
    }

    private CacheKey combineKeys(CacheKey rowKey, CacheKey parentRowKey) {
        if (rowKey.getUpdateCount() > 1 && parentRowKey.getUpdateCount() > 1) {
            CacheKey combinedKey;
            try {
                combinedKey = rowKey.clone();
            } catch (CloneNotSupportedException e) {
                throw new ExecutorException("Error cloning cache key.  Cause: " + e, e);
            }
            combinedKey.update(parentRowKey);
            return combinedKey;
        }
        return CacheKey.NULL_CACHE_KEY;
    }

    private List<ResultMapping> getResultMappingsForRowKey(ResultMap resultMap) {
        List<ResultMapping> resultMappings = resultMap.getIdResultMappings();
        if (resultMappings.isEmpty()) {
            resultMappings = resultMap.getPropertyResultMappings();
        }
        return resultMappings;
    }

    private void createRowKeyForMappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey,
                                                 List<ResultMapping> resultMappings, String columnPrefix) throws SQLException {
        for (ResultMapping resultMapping : resultMappings) {
            if (resultMapping.isSimple()) {
                final String column = prependPrefix(resultMapping.getColumn(), columnPrefix);
                final TypeHandler<?> th = resultMapping.getTypeHandler();
                Set<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);
                // Issue #114
                if (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH))) {
                    final Object value = th.getResult(rsw.getResultSet(), column);
                    if (value != null || configuration.isReturnInstanceForEmptyRow()) {
                        cacheKey.update(column);
                        cacheKey.update(value);
                    }
                }
            }
        }
    }

    private void createRowKeyForUnmappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey,
                                                   String columnPrefix) throws SQLException {
        final MetaClass metaType = MetaClass.forClass(resultMap.getType(), reflectorFactory);
        List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);
        for (String column : unmappedColumnNames) {
            String property = column;
            if (columnPrefix != null && !columnPrefix.isEmpty()) {
                // When columnPrefix is specified, ignore columns without the prefix.
                if (!column.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
                    continue;
                }
                property = column.substring(columnPrefix.length());
            }
            if (metaType.findProperty(property, configuration.isMapUnderscoreToCamelCase()) != null) {
                String value = rsw.getResultSet().getString(column);
                if (value != null) {
                    cacheKey.update(column);
                    cacheKey.update(value);
                }
            }
        }
    }

    private void createRowKeyForMap(ResultSetWrapper rsw, CacheKey cacheKey) throws SQLException {
        List<String> columnNames = rsw.getColumnNames();
        for (String columnName : columnNames) {
            final String value = rsw.getResultSet().getString(columnName);
            if (value != null) {
                cacheKey.update(columnName);
                cacheKey.update(value);
            }
        }
    }

    /**
     * 将对象链接到指定对象中
     * @param metaObject
     * @param resultMapping
     * @param rowValue
     */
    private void linkObjects(MetaObject metaObject, ResultMapping resultMapping, Object rowValue) {
        final Object collectionProperty = instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject);
        // 如果是collection类型，则直接把value通过add方法添加到属性中去
        if (collectionProperty != null) {
            final MetaObject targetMetaObject = configuration.newMetaObject(collectionProperty);
            targetMetaObject.add(rowValue);
        } else {
            // 否则使用 set 方法直接设置值
            metaObject.setValue(resultMapping.getProperty(), rowValue);
        }
    }

    /**
     * 如果该 ResultMapping 的类型是 List 时，实例化成 collection
     * 这里会判断目标属性值是否为空，如果未空则通过属性set的参数类型判断是否是collection，如果非空则直接判断值的value
     * @param resultMapping
     * @param metaObject
     * @return 如果目标属性类型是 collection 类型，则返回非空，如果非collection则返回null
     */
    private Object instantiateCollectionPropertyIfAppropriate(ResultMapping resultMapping, MetaObject metaObject) {
        final String propertyName = resultMapping.getProperty();
        Object propertyValue = metaObject.getValue(propertyName);
        if (propertyValue == null) {
            Class<?> type = resultMapping.getJavaType();
            if (type == null) {
                type = metaObject.getSetterType(propertyName);
            }
            try {
                if (objectFactory.isCollection(type)) {
                    propertyValue = objectFactory.create(type);
                    metaObject.setValue(propertyName, propertyValue);
                    return propertyValue;
                }
            } catch (Exception e) {
                throw new ExecutorException(
                        "Error instantiating collection property for result '" + resultMapping.getProperty() + "'.  Cause: " + e,
                        e);
            }
        } else if (objectFactory.isCollection(propertyValue.getClass())) {
            return propertyValue;
        }
        return null;
    }

    /**
     * 如果是唯一的字段，则直接跳过这个字段类型判断是否存在 TypeHandler，否则根据传入的类型判断是否存在 TypeHandler
     * @param rsw
     * @param resultType
     * @return
     */
    private boolean hasTypeHandlerForResultObject(ResultSetWrapper rsw, Class<?> resultType) {
        if (rsw.getColumnNames().size() == 1) {
            return typeHandlerRegistry.hasTypeHandler(resultType, rsw.getJdbcType(rsw.getColumnNames().get(0)));
        }
        return typeHandlerRegistry.hasTypeHandler(resultType);
    }

}
