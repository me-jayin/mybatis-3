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
package org.apache.ibatis.builder;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.decorators.LruCache;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.util.*;

/**
 * mapper建造协助类，提供命名空间校验、缓存引用等处理。
 * 每个 Mapper 都会构建一个该类对象，因此如果是 开启了二级缓存，那么 mapper 里面会共享 Cache
 *
 * @author Clinton Begin
 */
public class MapperBuilderAssistant extends BaseBuilder {

    private String currentNamespace;
    private final String resource;
    private Cache currentCache;
    private boolean unresolvedCacheRef; // issue #676

    public MapperBuilderAssistant(Configuration configuration, String resource) {
        super(configuration);
        ErrorContext.instance().resource(resource);
        this.resource = resource;
    }

    public String getCurrentNamespace() {
        return currentNamespace;
    }

    /**
     * 设置当前的命名空间
     * @param currentNamespace
     */
    public void setCurrentNamespace(String currentNamespace) {
        if (currentNamespace == null) {
            throw new BuilderException("The mapper element requires a namespace attribute to be specified.");
        }
        // 如果已经指定了命名空间，但后续提供的命名空间与原命名空间不一致，则抛出异常
        if (this.currentNamespace != null && !this.currentNamespace.equals(currentNamespace)) {
            throw new BuilderException(
                    "Wrong namespace. Expected '" + this.currentNamespace + "' but found '" + currentNamespace + "'.");
        }

        this.currentNamespace = currentNamespace;
    }

    /**
     * 应用当前namespace，如给 base 添加当前namespace路径
     * @param base
     * @param isReference 是否是引用其他地方的，如果是则base中包含 . 则直接返回，如果不是则会抛出异常
     * @return
     */
    public String applyCurrentNamespace(String base, boolean isReference) {
        if (base == null) {
            return null;
        }
        if (isReference) {
            // is it qualified with any namespace yet?
            if (base.contains(".")) {
                return base;
            }
        } else {
            // is it qualified with this namespace yet?
            if (base.startsWith(currentNamespace + ".")) {
                return base;
            }
            if (base.contains(".")) {
                throw new BuilderException("Dots are not allowed in element names, please remove it from " + base);
            }
        }
        return currentNamespace + "." + base;
    }

    /**
     * 登记缓存引用关系，并获取对应的 Cache 对象
     * @param namespace
     * @return
     */
    public Cache useCacheRef(String namespace) {
        if (namespace == null) {
            throw new BuilderException("cache-ref element requires a namespace attribute.");
        }
        try {
            unresolvedCacheRef = true;
            // 获取缓存，如果获取不到抛出异常
            Cache cache = configuration.getCache(namespace);
            if (cache == null) {
                throw new IncompleteElementException("No cache for namespace '" + namespace + "' could be found.");
            }
            // 取到了则更新缓存引用
            currentCache = cache;
            unresolvedCacheRef = false;
            return cache;
        } catch (IllegalArgumentException e) {
            throw new IncompleteElementException("No cache for namespace '" + namespace + "' could be found.", e);
        }
    }

    /**
     * 创建并注册新缓存对象
     * @param typeClass
     * @param evictionClass
     * @param flushInterval
     * @param size
     * @param readWrite
     * @param blocking
     * @param props
     * @return
     */
    public Cache useNewCache(Class<? extends Cache> typeClass, Class<? extends Cache> evictionClass, Long flushInterval,
                             Integer size, boolean readWrite, boolean blocking, Properties props) {
        Cache cache = new CacheBuilder(currentNamespace)
                .implementation(valueOrDefault(typeClass, PerpetualCache.class))
                .addDecorator(valueOrDefault(evictionClass, LruCache.class))
                .clearInterval(flushInterval)
                .size(size)
                .readWrite(readWrite)
                .blocking(blocking)
                .properties(props)
                .build();
        // 将缓存添加到配置中
        configuration.addCache(cache);
        currentCache = cache;
        return cache;
    }

    /**
     * 注册 ParameterMap
     * @param id
     * @param parameterClass
     * @param parameterMappings
     * @return
     */
    public ParameterMap addParameterMap(String id, Class<?> parameterClass, List<ParameterMapping> parameterMappings) {
        // 应用当前命名空间，如果id未指定命名空间则会将当前命名空间添加到前面去
        id = applyCurrentNamespace(id, false);
        ParameterMap parameterMap = new ParameterMap.Builder(configuration, id, parameterClass, parameterMappings).build();
        configuration.addParameterMap(parameterMap);
        return parameterMap;
    }

    /**
     * 构建 ParameterMap 对象
     * @param parameterType
     * @param property
     * @param javaType
     * @param jdbcType
     * @param resultMap
     * @param parameterMode
     * @param typeHandler
     * @param numericScale
     * @return
     */
    public ParameterMapping buildParameterMapping(Class<?> parameterType, String property, Class<?> javaType,
                                                  JdbcType jdbcType, String resultMap, ParameterMode parameterMode, Class<? extends TypeHandler<?>> typeHandler,
                                                  Integer numericScale) {
        resultMap = applyCurrentNamespace(resultMap, true);

        // Class parameterType = parameterMapBuilder.type();
        Class<?> javaTypeClass = resolveParameterJavaType(parameterType, property, javaType, jdbcType);
        TypeHandler<?> typeHandlerInstance = resolveTypeHandler(javaTypeClass, typeHandler);

        return new ParameterMapping.Builder(configuration, property, javaTypeClass).jdbcType(jdbcType)
                .resultMapId(resultMap).mode(parameterMode).numericScale(numericScale).typeHandler(typeHandlerInstance).build();
    }

    /**
     * 注册 ResultMap 对象
     * @param id
     * @param type
     * @param extend
     * @param discriminator
     * @param resultMappings
     * @param autoMapping
     * @return
     */
    public ResultMap addResultMap(String id, Class<?> type, String extend, Discriminator discriminator,
                                  List<ResultMapping> resultMappings, Boolean autoMapping) {
        id = applyCurrentNamespace(id, false);
        extend = applyCurrentNamespace(extend, true);

        // 是否需要继承对应的 resultMap
        if (extend != null) {
            // 如果父ResultMap 还未初始化，则抛出异常，将其标记为未完成加载的 ResultMap
            if (!configuration.hasResultMap(extend)) {
                throw new IncompleteElementException("Could not find a parent resultmap with id '" + extend + "'");
            }
            // 获取父配置
            ResultMap resultMap = configuration.getResultMap(extend);
            List<ResultMapping> extendedResultMappings = new ArrayList<>(resultMap.getResultMappings());
            extendedResultMappings.removeAll(resultMappings);
            // 如果当前定义了构造器配置，则需要移除父配置中的构造器配置
            boolean declaresConstructor = false;
            for (ResultMapping resultMapping : resultMappings) {
                if (resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR)) {
                    declaresConstructor = true;
                    break;
                }
            }
            if (declaresConstructor) {
                extendedResultMappings.removeIf(resultMapping -> resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR));
            }
            // 应用父配置
            resultMappings.addAll(extendedResultMappings);
        }
        // 构建并注册ResultMap对象
        ResultMap resultMap = new ResultMap.Builder(configuration, id, type, resultMappings, autoMapping)
                .discriminator(discriminator).build();
        configuration.addResultMap(resultMap);
        return resultMap;
    }

    /**
     * 构建一个 Discriminator 对象
     * @param resultType
     * @param column
     * @param javaType
     * @param jdbcType
     * @param typeHandler
     * @param discriminatorMap
     * @return
     */
    public Discriminator buildDiscriminator(Class<?> resultType, String column, Class<?> javaType, JdbcType jdbcType,
                                            Class<? extends TypeHandler<?>> typeHandler, Map<String, String> discriminatorMap) {
        // 将 <discriminator> 标签的属性存放到一个 ResultMapping 中。
        // 如：使用哪个字段进行case比较操作
        ResultMapping resultMapping = buildResultMapping(resultType, null, column, javaType, jdbcType, null, null, null,
                null, typeHandler, new ArrayList<>(), null, null, false);
        Map<String, String> namespaceDiscriminatorMap = new HashMap<>();
        for (Map.Entry<String, String> e : discriminatorMap.entrySet()) {
            // 对 value - resultMapId 进行处理，将不合法的 resultMapId 加上 当前的namespace
            // 因为 <case resultMap> 的情况下，可能是引用了当前namespace内的 resultMap，因此可能缺少namespace前缀
            String resultMap = e.getValue();
            resultMap = applyCurrentNamespace(resultMap, true);
            namespaceDiscriminatorMap.put(e.getKey(), resultMap);
        }
        // 开始构建
        return new Discriminator.Builder(configuration, resultMapping, namespaceDiscriminatorMap).build();
    }

    /**
     * 添加 MappedStatement ，即 insert、update、select、delete 标签解析结果
     * @param id
     * @param sqlSource SqlSource
     * @param statementType Statement类型，如 STATEMENT, PREPARED, CALLABLE
     * @param sqlCommandType sql 语句类型，如INSERT、UPDATE等
     * @param fetchSize 拉取数据数量
     * @param timeout 执行超时时长
     * @param parameterMap 引用的 parameterMap 的id
     * @param parameterType 入参类型
     * @param resultMap resultMap id
     * @param resultType 返回类型
     * @param resultSetType
     * @param flushCache 是否清除缓存
     * @param useCache 是否使用缓存
     * @param resultOrdered 结果是否已经进行排序
     * @param keyGenerator key生成器
     * @param keyProperty 主键对应的属性
     * @param keyColumn 主键取值的字段名
     * @param databaseId 生效的数据库标识
     * @param lang LanguageDriver，分为 XML（动态）、RAW（静态）
     * @param resultSets
     * @param dirtySelect
     * @return
     */
    public MappedStatement addMappedStatement(String id, SqlSource sqlSource, StatementType statementType,
                                              SqlCommandType sqlCommandType, Integer fetchSize, Integer timeout, String parameterMap, Class<?> parameterType,
                                              String resultMap, Class<?> resultType, ResultSetType resultSetType, boolean flushCache, boolean useCache,
                                              boolean resultOrdered, KeyGenerator keyGenerator, String keyProperty, String keyColumn, String databaseId,
                                              LanguageDriver lang, String resultSets, boolean dirtySelect) {
        // 如果 引用的缓存 还为初始化好，则直接将当前操作挂到队列中
        if (unresolvedCacheRef) {
            throw new IncompleteElementException("Cache-ref not yet resolved");
        }

        id = applyCurrentNamespace(id, false);

        MappedStatement.Builder statementBuilder = new MappedStatement.Builder(configuration, id, sqlSource, sqlCommandType)
                .resource(resource).fetchSize(fetchSize).timeout(timeout).statementType(statementType)
                .keyGenerator(keyGenerator).keyProperty(keyProperty).keyColumn(keyColumn).databaseId(databaseId).lang(lang)
                .resultOrdered(resultOrdered).resultSets(resultSets)
                .resultMaps(getStatementResultMaps(resultMap, resultType, id)).resultSetType(resultSetType)
                .flushCacheRequired(flushCache).useCache(useCache).cache(currentCache).dirtySelect(dirtySelect);
        // 根据id
        ParameterMap statementParameterMap = getStatementParameterMap(parameterMap, parameterType, id);
        if (statementParameterMap != null) {
            statementBuilder.parameterMap(statementParameterMap);
        }

        // 构建并注册 MappedStatement 对象
        MappedStatement statement = statementBuilder.build();
        configuration.addMappedStatement(statement);
        return statement;
    }

    /**
     * Backward compatibility signature 'addMappedStatement'.
     *
     * @param id             the id
     * @param sqlSource      the sql source
     * @param statementType  the statement type
     * @param sqlCommandType the sql command type
     * @param fetchSize      the fetch size
     * @param timeout        the timeout
     * @param parameterMap   the parameter map
     * @param parameterType  the parameter type
     * @param resultMap      the result map
     * @param resultType     the result type
     * @param resultSetType  the result set type
     * @param flushCache     the flush cache
     * @param useCache       the use cache
     * @param resultOrdered  the result ordered
     * @param keyGenerator   the key generator
     * @param keyProperty    the key property
     * @param keyColumn      the key column
     * @param databaseId     the database id
     * @param lang           the lang
     * @return the mapped statement
     */
    public MappedStatement addMappedStatement(String id, SqlSource sqlSource, StatementType statementType,
                                              SqlCommandType sqlCommandType, Integer fetchSize, Integer timeout, String parameterMap, Class<?> parameterType,
                                              String resultMap, Class<?> resultType, ResultSetType resultSetType, boolean flushCache, boolean useCache,
                                              boolean resultOrdered, KeyGenerator keyGenerator, String keyProperty, String keyColumn, String databaseId,
                                              LanguageDriver lang, String resultSets) {
        return addMappedStatement(id, sqlSource, statementType, sqlCommandType, fetchSize, timeout, parameterMap,
                parameterType, resultMap, resultType, resultSetType, flushCache, useCache, resultOrdered, keyGenerator,
                keyProperty, keyColumn, databaseId, lang, null, false);
    }

    public MappedStatement addMappedStatement(String id, SqlSource sqlSource, StatementType statementType,
                                              SqlCommandType sqlCommandType, Integer fetchSize, Integer timeout, String parameterMap, Class<?> parameterType,
                                              String resultMap, Class<?> resultType, ResultSetType resultSetType, boolean flushCache, boolean useCache,
                                              boolean resultOrdered, KeyGenerator keyGenerator, String keyProperty, String keyColumn, String databaseId,
                                              LanguageDriver lang) {
        return addMappedStatement(id, sqlSource, statementType, sqlCommandType, fetchSize, timeout, parameterMap,
                parameterType, resultMap, resultType, resultSetType, flushCache, useCache, resultOrdered, keyGenerator,
                keyProperty, keyColumn, databaseId, lang, null);
    }

    private <T> T valueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * 获取 ParameterMap 对象
     * @param parameterMapName
     * @param parameterTypeClass
     * @param statementId
     * @return
     */
    private ParameterMap getStatementParameterMap(String parameterMapName, Class<?> parameterTypeClass,
                                                  String statementId) {
        parameterMapName = applyCurrentNamespace(parameterMapName, true);
        ParameterMap parameterMap = null;
        if (parameterMapName != null) { // 如果指定 parameterMap id 则直接获取
            try {
                parameterMap = configuration.getParameterMap(parameterMapName);
            } catch (IllegalArgumentException e) {
                throw new IncompleteElementException("Could not find parameter map " + parameterMapName, e);
            }
        } else if (parameterTypeClass != null) { // 如果指定了参数类型，则通过参数类型构建內部的 ParameterType
            List<ParameterMapping> parameterMappings = new ArrayList<>();
            parameterMap = new ParameterMap.Builder(configuration, statementId + "-Inline", parameterTypeClass,
                    parameterMappings).build();
        }
        return parameterMap;
    }

    /**
     * 获取 statement 响应 ResultMap ，由于mybatis能够支持多结果集，因此 resultMap 可以使用','分隔，用于描述每个结果集的 resultMap
     * @param resultMap
     * @param resultType
     * @param statementId
     * @return
     */
    private List<ResultMap> getStatementResultMaps(String resultMap, Class<?> resultType, String statementId) {
        resultMap = applyCurrentNamespace(resultMap, true);

        List<ResultMap> resultMaps = new ArrayList<>();
        if (resultMap != null) {
            // 切割
            String[] resultMapNames = resultMap.split(",");
            for (String resultMapName : resultMapNames) {
                try {
                    // 从 confugiration 中获取 resultMap
                    resultMaps.add(configuration.getResultMap(resultMapName.trim()));
                } catch (IllegalArgumentException e) {
                    throw new IncompleteElementException(
                            "Could not find result map '" + resultMapName + "' referenced from '" + statementId + "'", e);
                }
            }
        } else if (resultType != null) {
            // 如果使用 resultType 来作为返回值，则创建一个内部 ReseultMap
            ResultMap inlineResultMap = new ResultMap.Builder(configuration, statementId + "-Inline", resultType,
                    new ArrayList<>(), null).build();
            resultMaps.add(inlineResultMap);
        }
        return resultMaps;
    }

    /**
     * 构建ResultMapping对象
     * @param resultType 结果类型
     * @param property 参数名
     * @param column 字段名
     * @param javaType 对应的java类型
     * @param jdbcType jdbc类型
     * @param nestedSelect 是否嵌套sql查询
     * @param nestedResultMap 是否嵌套结果集，例如 <collection/> <association/> 等通过子标签声明结构的方式，都会视作嵌套一个特殊的resultMap
     * @param notNullColumn
     * @param columnPrefix 字段前缀
     * @param typeHandler 当前的类型处理器
     * @param flags 标识
     * @param resultSet
     * @param foreignColumn 外键，用于对比装配值，如 collection 会根据该属性指定的值，来决定归属于哪个对象
     * @param lazy 是否延迟加载
     * @return
     */
    public ResultMapping buildResultMapping(Class<?> resultType, String property, String column, Class<?> javaType,
                                            JdbcType jdbcType, String nestedSelect, String nestedResultMap, String notNullColumn, String columnPrefix,
                                            Class<? extends TypeHandler<?>> typeHandler, List<ResultFlag> flags, String resultSet, String foreignColumn,
                                            boolean lazy) {
        Class<?> javaTypeClass = resolveResultJavaType(resultType, property, javaType);
        TypeHandler<?> typeHandlerInstance = resolveTypeHandler(javaTypeClass, typeHandler);
        List<ResultMapping> composites;
        // 处理嵌套查询操作
        if ((nestedSelect == null || nestedSelect.isEmpty()) && (foreignColumn == null || foreignColumn.isEmpty())) {
            composites = Collections.emptyList();
        } else {
            composites = parseCompositeColumnName(column);
        }
        return new ResultMapping.Builder(configuration, property, column, javaTypeClass).jdbcType(jdbcType)
                .nestedQueryId(applyCurrentNamespace(nestedSelect, true))
                .nestedResultMapId(applyCurrentNamespace(nestedResultMap, true)).resultSet(resultSet)
                .typeHandler(typeHandlerInstance).flags(flags == null ? new ArrayList<>() : flags).composites(composites)
                .notNullColumns(parseMultipleColumnNames(notNullColumn)).columnPrefix(columnPrefix).foreignColumn(foreignColumn)
                .lazy(lazy).build();
    }

    /**
     * Backward compatibility signature 'buildResultMapping'.
     *
     * @param resultType      the result type
     * @param property        the property
     * @param column          the column
     * @param javaType        the java type
     * @param jdbcType        the jdbc type
     * @param nestedSelect    the nested select
     * @param nestedResultMap the nested result map
     * @param notNullColumn   the not null column
     * @param columnPrefix    the column prefix
     * @param typeHandler     the type handler
     * @param flags           the flags
     * @return the result mapping
     */
    public ResultMapping buildResultMapping(Class<?> resultType, String property, String column, Class<?> javaType,
                                            JdbcType jdbcType, String nestedSelect, String nestedResultMap, String notNullColumn, String columnPrefix,
                                            Class<? extends TypeHandler<?>> typeHandler, List<ResultFlag> flags) {
        return buildResultMapping(resultType, property, column, javaType, jdbcType, nestedSelect, nestedResultMap,
                notNullColumn, columnPrefix, typeHandler, flags, null, null, configuration.isLazyLoadingEnabled());
    }

    /**
     * Gets the language driver.
     *
     * @param langClass the lang class
     * @return the language driver
     * @deprecated Use {@link Configuration#getLanguageDriver(Class)}
     */
    @Deprecated
    public LanguageDriver getLanguageDriver(Class<? extends LanguageDriver> langClass) {
        return configuration.getLanguageDriver(langClass);
    }

    private Set<String> parseMultipleColumnNames(String columnName) {
        Set<String> columns = new HashSet<>();
        if (columnName != null) {
            if (columnName.indexOf(',') > -1) {
                StringTokenizer parser = new StringTokenizer(columnName, "{}, ", false);
                while (parser.hasMoreTokens()) {
                    String column = parser.nextToken();
                    columns.add(column);
                }
            } else {
                columns.add(columnName);
            }
        }
        return columns;
    }

    /**
     * 解析字段表达式，如嵌套查询时， column 可以使用表达式：{id=group_id,state=state}
     * 对查询出来的结果字段名称进行重命名，如将 group_id 映射为 id
     * @param columnName
     * @return
     */
    private List<ResultMapping> parseCompositeColumnName(String columnName) {
        List<ResultMapping> composites = new ArrayList<>();
        if (columnName != null && (columnName.indexOf('=') > -1 || columnName.indexOf(',') > -1)) {
            StringTokenizer parser = new StringTokenizer(columnName, "{}=, ", false);
            while (parser.hasMoreTokens()) {
                String property = parser.nextToken();
                String column = parser.nextToken();
                ResultMapping complexResultMapping = new ResultMapping.Builder(configuration, property, column,
                        configuration.getTypeHandlerRegistry().getUnknownTypeHandler()).build();
                composites.add(complexResultMapping);
            }
        }
        return composites;
    }

    private Class<?> resolveResultJavaType(Class<?> resultType, String property, Class<?> javaType) {
        if (javaType == null && property != null) {
            try {
                MetaClass metaResultType = MetaClass.forClass(resultType, configuration.getReflectorFactory());
                javaType = metaResultType.getSetterType(property);
            } catch (Exception e) {
                // ignore, following null check statement will deal with the situation
            }
        }
        if (javaType == null) {
            javaType = Object.class;
        }
        return javaType;
    }

    private Class<?> resolveParameterJavaType(Class<?> resultType, String property, Class<?> javaType,
                                              JdbcType jdbcType) {
        if (javaType == null) {
            if (JdbcType.CURSOR.equals(jdbcType)) {
                javaType = java.sql.ResultSet.class;
            } else if (Map.class.isAssignableFrom(resultType)) {
                javaType = Object.class;
            } else {
                MetaClass metaResultType = MetaClass.forClass(resultType, configuration.getReflectorFactory());
                javaType = metaResultType.getGetterType(property);
            }
        }
        if (javaType == null) {
            javaType = Object.class;
        }
        return javaType;
    }

}
