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
package org.apache.ibatis.binding;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 重点！！！
 * mapper的方法，作为正常 mapper 方法的调用入口
 * 内部持有属性：
 * 1.SqlCommand sql命令对象，存有该方法的 MappedStatement id 和 statement 的sql语句类型，如 SELECT、UPDATE等
 * 2.MethodSignature 方法签名，对方法的描述，如方法响应类型的描述、RowBounds、ResultHandler以及参数名称解析器
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 * @author Kazuki Shimizu
 */
public class MapperMethod {

    private final SqlCommand command;
    private final MethodSignature method;

    public MapperMethod(Class<?> mapperInterface, Method method, Configuration config) {
        // 构建 sql 命令对象
        this.command = new SqlCommand(config, mapperInterface, method);
        // 方法的签名值
        this.method = new MethodSignature(config, mapperInterface, method);
    }

    /**
     * 重点！！！
     * 执行对应的 Mapper 方法，也就是进行实际的调用
     * @param sqlSession
     * @param args
     * @return
     */
    public Object execute(SqlSession sqlSession, Object[] args) {
        Object result;
        switch (command.getType()) {
            // insert和delete 类型的操作实际上都会委托给 update
            case INSERT: {
                Object param = method.convertArgsToSqlCommandParam(args);
                result = rowCountResult(sqlSession.insert(command.getName(), param));
                break;
            }
            case UPDATE: {
                Object param = method.convertArgsToSqlCommandParam(args);
                result = rowCountResult(sqlSession.update(command.getName(), param));
                break;
            }
            case DELETE: {
                Object param = method.convertArgsToSqlCommandParam(args);
                result = rowCountResult(sqlSession.delete(command.getName(), param));
                break;
            }
            case SELECT: // sql执行中最重要的部分
                // 如果无返回值，并且参数中存在 ResultHandler 类型，则直接执行，响应结果交由 ResultHandler 处理
                if (method.returnsVoid() && method.hasResultHandler()) {
                    executeWithResultHandler(sqlSession, args);
                    result = null;
                } else if (method.returnsMany()) { // 如果是返回多数据集
                    result = executeForMany(sqlSession, args);
                } else if (method.returnsMap()) { // 如果是返回map
                    result = executeForMap(sqlSession, args);
                } else if (method.returnsCursor()) { // 如果是返回 cursor游标
                    result = executeForCursor(sqlSession, args);
                } else { // 其他类型统一当成单结果处理
                    Object param = method.convertArgsToSqlCommandParam(args);
                    result = sqlSession.selectOne(command.getName(), param);
                    if (method.returnsOptional() && (result == null || !method.getReturnType().equals(result.getClass()))) {
                        result = Optional.ofNullable(result);
                    }
                }
                break;
            case FLUSH: // 清除 statement 操作，例如 reuse、batch 类型的 Executor 内部会持有 Statement 引用，使用该 类型 就能将其关闭并清除
                result = sqlSession.flushStatements();
                break;
            default:
                throw new BindingException("Unknown execution method for: " + command.getName());
        }
        if (result == null && method.getReturnType().isPrimitive() && !method.returnsVoid()) {
            throw new BindingException("Mapper method '" + command.getName()
                    + "' attempted to return null from a method with a primitive return type (" + method.getReturnType() + ").");
        }
        return result;
    }

    /**
     * 处理数据影响的结果响应 int值，将其转成对应类型数据，如转成boolean或者Long
     * @param rowCount
     * @return
     */
    private Object rowCountResult(int rowCount) {
        final Object result;
        if (method.returnsVoid()) {
            result = null;
        } else if (Integer.class.equals(method.getReturnType()) || Integer.TYPE.equals(method.getReturnType())) {
            result = rowCount;
        } else if (Long.class.equals(method.getReturnType()) || Long.TYPE.equals(method.getReturnType())) {
            result = (long) rowCount;
        } else if (Boolean.class.equals(method.getReturnType()) || Boolean.TYPE.equals(method.getReturnType())) {
            result = rowCount > 0;
        } else {
            throw new BindingException(
                    "Mapper method '" + command.getName() + "' has an unsupported return type: " + method.getReturnType());
        }
        return result;
    }

    /**
     * 执行语句，但结果通过 ResultHandler 来处理
     * @param sqlSession
     * @param args
     */
    private void executeWithResultHandler(SqlSession sqlSession, Object[] args) {
        MappedStatement ms = sqlSession.getConfiguration().getMappedStatement(command.getName());
        // 不支持无返回值并且不是调用存储过程的方式调用
        if (!StatementType.CALLABLE.equals(ms.getStatementType())
                && void.class.equals(ms.getResultMaps().get(0).getType())) {
            throw new BindingException(
                    "method " + command.getName() + " needs either a @ResultMap annotation, a @ResultType annotation,"
                            + " or a resultType attribute in XML so a ResultHandler can be used as a parameter.");
        }
        // 对参数进行处理
        Object param = method.convertArgsToSqlCommandParam(args);
        // 判断入参是否存在 RowBounds，如果存在则表示需要限制行
        if (method.hasRowBounds()) {
            // 获取 RowBounds 参数
            RowBounds rowBounds = method.extractRowBounds(args);
            // 直接查询
            sqlSession.select(command.getName(), param, rowBounds, method.extractResultHandler(args));
        } else {
            sqlSession.select(command.getName(), param, method.extractResultHandler(args));
        }
    }

    private <E> Object executeForMany(SqlSession sqlSession, Object[] args) {
        List<E> result;
        Object param = method.convertArgsToSqlCommandParam(args);
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.selectList(command.getName(), param, rowBounds);
        } else {
            result = sqlSession.selectList(command.getName(), param);
        }
        // issue #510 Collections & arrays support
        if (!method.getReturnType().isAssignableFrom(result.getClass())) {
            if (method.getReturnType().isArray()) {
                return convertToArray(result);
            }
            return convertToDeclaredCollection(sqlSession.getConfiguration(), result);
        }
        return result;
    }

    private <T> Cursor<T> executeForCursor(SqlSession sqlSession, Object[] args) {
        Cursor<T> result;
        Object param = method.convertArgsToSqlCommandParam(args);
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.selectCursor(command.getName(), param, rowBounds);
        } else {
            result = sqlSession.selectCursor(command.getName(), param);
        }
        return result;
    }

    private <E> Object convertToDeclaredCollection(Configuration config, List<E> list) {
        Object collection = config.getObjectFactory().create(method.getReturnType());
        MetaObject metaObject = config.newMetaObject(collection);
        metaObject.addAll(list);
        return collection;
    }

    @SuppressWarnings("unchecked")
    private <E> Object convertToArray(List<E> list) {
        Class<?> arrayComponentType = method.getReturnType().getComponentType();
        Object array = Array.newInstance(arrayComponentType, list.size());
        if (!arrayComponentType.isPrimitive()) {
            return list.toArray((E[]) array);
        }
        for (int i = 0; i < list.size(); i++) {
            Array.set(array, i, list.get(i));
        }
        return array;
    }

    private <K, V> Map<K, V> executeForMap(SqlSession sqlSession, Object[] args) {
        Map<K, V> result;
        Object param = method.convertArgsToSqlCommandParam(args);
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.selectMap(command.getName(), param, method.getMapKey(), rowBounds);
        } else {
            result = sqlSession.selectMap(command.getName(), param, method.getMapKey());
        }
        return result;
    }

    public static class ParamMap<V> extends HashMap<String, V> {

        private static final long serialVersionUID = -2212268410512043556L;

        @Override
        public V get(Object key) {
            if (!super.containsKey(key)) {
                throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + keySet());
            }
            return super.get(key);
        }

    }

    /**
     * sql命令对象，内部持有MappedStatement的id和sql命令类型（如INSERT、UPDATE等）
     */
    public static class SqlCommand {

        private final String name;
        private final SqlCommandType type;

        public SqlCommand(Configuration configuration, Class<?> mapperInterface, Method method) {
            // 获取目标方法的 方法名、定义 的类
            final String methodName = method.getName();
            final Class<?> declaringClass = method.getDeclaringClass();
            // 解析获取方法对应的 MappedStatement
            MappedStatement ms = resolveMappedStatement(mapperInterface, methodName, declaringClass, configuration);

            if (ms == null) {
                // 如果是 Flush 清空缓存的方法，则无需 MappedStatement
                if (method.getAnnotation(Flush.class) == null) {
                    throw new BindingException(
                            "Invalid bound statement (not found): " + mapperInterface.getName() + "." + methodName);
                }
                name = null;
                type = SqlCommandType.FLUSH;
            } else {
                // 将 name 设置为 MappedStatement 的 id
                name = ms.getId();
                type = ms.getSqlCommandType();
                // 如果未知的 type 则抛出异常
                if (type == SqlCommandType.UNKNOWN) {
                    throw new BindingException("Unknown execution method for: " + name);
                }
            }
        }

        /**
         * 实际上就是方法的全访问限制名
         * @return
         */
        public String getName() {
            return name;
        }

        public SqlCommandType getType() {
            return type;
        }

        /**
         * 根据 mapper 接口，及方法名和方法定义的类，解析获取对应的 MappedStatement
         *
         * @param mapperInterface
         * @param methodName
         * @param declaringClass
         * @param configuration
         * @return
         */
        private MappedStatement resolveMappedStatement(Class<?> mapperInterface, String methodName, Class<?> declaringClass,
                                                       Configuration configuration) {
            // 默认情况下，如果 A extends B，然后调用 B 定义的方法 method1 时，会先尝试通过 A.method1 来查找 MappedStatement
            // 因为可能 A 在 mapper.xml 对 method1 添加了sql标签
            String statementId = mapperInterface.getName() + "." + methodName;
            if (configuration.hasStatement(statementId)) {
                return configuration.getMappedStatement(statementId);
            }
            // 如果方法就是 A 定义的方法，则直接返回null表示未找到
            if (mapperInterface.equals(declaringClass)) {
                return null;
            }
            // 如果通过 A.method1 找不到对应的 MappedStatement，那么将获取继承的类
            // 并找到方法定义类的子类，进行递归解析 MappedStatement
            for (Class<?> superInterface : mapperInterface.getInterfaces()) {
                if (declaringClass.isAssignableFrom(superInterface)) {
                    MappedStatement ms = resolveMappedStatement(superInterface, methodName, declaringClass, configuration);
                    if (ms != null) {
                        return ms;
                    }
                }
            }
            return null;
        }
    }

    /**
     * method 签名对象，即对 method 进行解析后的签名值，会预先解析准备好方法返回是否是 list、是否返回map、是否返回void 等，
     * 最重要的是持有方法入参的映射关系
     */
    public static class MethodSignature {
        /**
         * 是否返回多数据
         */
        private final boolean returnsMany;
        /**
         * 是否返回map
         */
        private final boolean returnsMap;
        /**
         * 是否返回void
         */
        private final boolean returnsVoid;
        /**
         * 是否返回 cursor
         */
        private final boolean returnsCursor;
        /**
         * 是否返回optional
         */
        private final boolean returnsOptional;
        /**
         * 返回类型
         */
        private final Class<?> returnType;
        /**
         * map的key
         */
        private final String mapKey;
        /**
         * 记录入参中 ResultHandler 在参数里的下标
         */
        private final Integer resultHandlerIndex;
        /**
         *
         */
        private final Integer rowBoundsIndex;
        /**
         * 参数名称解析器
         */
        private final ParamNameResolver paramNameResolver;

        /**
         * 对方法进行解析的签名
         *
         * @param configuration
         * @param mapperInterface
         * @param method
         */
        public MethodSignature(Configuration configuration, Class<?> mapperInterface, Method method) {
            // 解析返回泛型的类型
            Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, mapperInterface);
            if (resolvedReturnType instanceof Class<?>) {
                this.returnType = (Class<?>) resolvedReturnType;
            } else if (resolvedReturnType instanceof ParameterizedType) {
                this.returnType = (Class<?>) ((ParameterizedType) resolvedReturnType).getRawType();
            } else {
                this.returnType = method.getReturnType();
            }
            // 判断响应类型
            this.returnsVoid = void.class.equals(this.returnType);
            this.returnsMany = configuration.getObjectFactory().isCollection(this.returnType) || this.returnType.isArray();
            this.returnsCursor = Cursor.class.equals(this.returnType);
            this.returnsOptional = Optional.class.equals(this.returnType);

            this.mapKey = getMapKey(method);
            this.returnsMap = this.mapKey != null;

            // 获取 RowBounds、ResultHandler 的参数下标
            this.rowBoundsIndex = getUniqueParamIndex(method, RowBounds.class);
            this.resultHandlerIndex = getUniqueParamIndex(method, ResultHandler.class);

            // 创建参数名称解析器
            this.paramNameResolver = new ParamNameResolver(configuration, method);
        }

        /**
         * 将参数数组转换为 参数名-参数值 的入参
         * @param args
         * @return
         */
        public Object convertArgsToSqlCommandParam(Object[] args) {
            return paramNameResolver.getNamedParams(args);
        }

        public boolean hasRowBounds() {
            return rowBoundsIndex != null;
        }

        public RowBounds extractRowBounds(Object[] args) {
            return hasRowBounds() ? (RowBounds) args[rowBoundsIndex] : null;
        }

        public boolean hasResultHandler() {
            return resultHandlerIndex != null;
        }

        /**
         * 获取参数中的 ResultHandler 对象
         * @param args
         * @return
         */
        public ResultHandler extractResultHandler(Object[] args) {
            return hasResultHandler() ? (ResultHandler) args[resultHandlerIndex] : null;
        }

        public Class<?> getReturnType() {
            return returnType;
        }

        public boolean returnsMany() {
            return returnsMany;
        }

        public boolean returnsMap() {
            return returnsMap;
        }

        public boolean returnsVoid() {
            return returnsVoid;
        }

        public boolean returnsCursor() {
            return returnsCursor;
        }

        /**
         * return whether return type is {@code java.util.Optional}.
         *
         * @return return {@code true}, if return type is {@code java.util.Optional}
         * @since 3.5.0
         */
        public boolean returnsOptional() {
            return returnsOptional;
        }

        /**
         * 获取指定类型的参数下标，并且校验该类型只出现一次
         * @param method
         * @param paramType
         * @return
         */
        private Integer getUniqueParamIndex(Method method, Class<?> paramType) {
            Integer index = null;
            final Class<?>[] argTypes = method.getParameterTypes();
            for (int i = 0; i < argTypes.length; i++) {
                if (paramType.isAssignableFrom(argTypes[i])) {
                    if (index != null) {
                        throw new BindingException(
                                method.getName() + " cannot have multiple " + paramType.getSimpleName() + " parameters");
                    }
                    index = i;
                }
            }
            return index;
        }

        public String getMapKey() {
            return mapKey;
        }

        /**
         * 如果响应是 map 时，获取 MapKey 指定的key值
         * @param method
         * @return
         */
        private String getMapKey(Method method) {
            String mapKey = null;
            // 如果响应类型是 Map，则获取 MapKey 指定的key值
            if (Map.class.isAssignableFrom(method.getReturnType())) {
                final MapKey mapKeyAnnotation = method.getAnnotation(MapKey.class);
                if (mapKeyAnnotation != null) {
                    mapKey = mapKeyAnnotation.value();
                }
            }
            return mapKey;
        }
    }

}
