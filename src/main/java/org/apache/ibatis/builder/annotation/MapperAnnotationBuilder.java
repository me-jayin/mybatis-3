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
package org.apache.ibatis.builder.annotation;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.UnknownTypeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class MapperAnnotationBuilder {

    private static final Set<Class<? extends Annotation>> statementAnnotationTypes = Stream
            .of(Select.class, Update.class, Insert.class, Delete.class, SelectProvider.class, UpdateProvider.class,
                    InsertProvider.class, DeleteProvider.class)
            .collect(Collectors.toSet());

    private final Configuration configuration;
    private final MapperBuilderAssistant assistant;
    private final Class<?> type;

    public MapperAnnotationBuilder(Configuration configuration, Class<?> type) {
        String resource = type.getName().replace('.', '/') + ".java (best guess)";
        this.assistant = new MapperBuilderAssistant(configuration, resource);
        this.configuration = configuration;
        this.type = type;
    }

    public void parse() {
        String resource = type.toString();
        // 判断指定类对应的资源是否加载
        if (!configuration.isResourceLoaded(resource)) {
            // 加载xml
            loadXmlResource();
            // 标记该resource已加载
            configuration.addLoadedResource(resource);
            // 设置命名空间为指定 mapper 类
            assistant.setCurrentNamespace(type.getName());
            // 解析缓存
            parseCache();
            // 解析 cache-ref 缓存引用
            parseCacheRef();
            // 获取所有的方法
            for (Method method : type.getMethods()) {
                // 跳 过桥 接和 default 方法，也表明 @Select 等注解不能使用在 default
                if (!canHaveStatement(method)) {
                    continue;
                }
                // 判断是否存在 select 标注的方法，并且方法未指定 ResultMap 时，则先根据方法返回类型，生成 Inline 的 ResultMap
                if (getAnnotationWrapper(method, false, Select.class, SelectProvider.class).isPresent()
                        && method.getAnnotation(ResultMap.class) == null) {
                    parseResultMap(method);
                }
                try {
                    parseStatement(method);
                } catch (IncompleteElementException e) {
                    // 对未完成的元素进行包装，添加 MethodResolver，实际上就是调用 parseResultMap 方法
                    configuration.addIncompleteMethod(new MethodResolver(this, method));
                }
            }
        }
        // 等所有 mapper 接口类解析完成后，继续执行未解析完成的 method
        configuration.parsePendingMethods(false);
    }

    private static boolean canHaveStatement(Method method) {
        // issue #237
        return !method.isBridge() && !method.isDefault();
    }

    /**
     * 加载对应的xml资源
     */
    private void loadXmlResource() {
        // Spring may not know the real resource name so we check a flag
        // to prevent loading again a resource twice
        // this flag is set at XMLMapperBuilder#bindMapperForNamespace
        // 用于判断该mapper对应的xml的资源是否已经加载完成，用于兼容spring的方式，在xml解析完成时，也会往configuration中添加 namespace:xxx 的标识
        // org.apache.ibatis.builder.xml.XMLMapperBuilder.bindMapperForNamespace()
        if (!configuration.isResourceLoaded("namespace:" + type.getName())) { // 如果xml未加载
            // 根据类全路径，得到对应的xml配置路径，默认情况下xml是放在与mapper同一个目录下的
            String xmlResource = type.getName().replace('.', '/') + ".xml";
            // 加载资源
            InputStream inputStream = type.getResourceAsStream("/" + xmlResource);
            if (inputStream == null) {
                // Search XML mapper that is not in the module but in the classpath.
                try {
                    inputStream = Resources.getResourceAsStream(type.getClassLoader(), xmlResource);
                } catch (IOException e2) {
                    // ignore, resource is not required
                }
            }
            // 执行mapper.xml的解析
            if (inputStream != null) {
                XMLMapperBuilder xmlParser = new XMLMapperBuilder(inputStream, assistant.getConfiguration(), xmlResource,
                        configuration.getSqlFragments(), type.getName());
                xmlParser.parse();
            }
        }
    }

    /**
     * 解析 mybatis 的缓存，即解析 {@link CacheNamespace}。注意如果 xml 和 注解同时存在缓存配置时，则注解优先级更高，会覆盖xml的方式
     */
    private void parseCache() {
        // 获取 CacheNamespace 注解描述值
        CacheNamespace cacheDomain = type.getAnnotation(CacheNamespace.class);
        if (cacheDomain != null) {
            Integer size = cacheDomain.size() == 0 ? null : cacheDomain.size();
            Long flushInterval = cacheDomain.flushInterval() == 0 ? null : cacheDomain.flushInterval();
            Properties props = convertToProperties(cacheDomain.properties());
            // 构建缓存对象
            assistant.useNewCache(cacheDomain.implementation(), cacheDomain.eviction(), flushInterval, size,
                    cacheDomain.readWrite(), cacheDomain.blocking(), props);
        }
    }

    /**
     * 将 {@link Property} 注解组转成 Properties配置
     *
     * @param properties
     * @return
     */
    private Properties convertToProperties(Property[] properties) {
        if (properties.length == 0) {
            return null;
        }
        Properties props = new Properties();
        for (Property property : properties) {
            props.setProperty(property.name(), PropertyParser.parse(property.value(), configuration.getVariables()));
        }
        return props;
    }

    /**
     * 解析 CacheRef 引用
     */
    private void parseCacheRef() {
        CacheNamespaceRef cacheDomainRef = type.getAnnotation(CacheNamespaceRef.class);
        if (cacheDomainRef != null) {
            Class<?> refType = cacheDomainRef.value();
            String refName = cacheDomainRef.name();
            if (refType == void.class && refName.isEmpty()) {
                throw new BuilderException("Should be specified either value() or name() attribute in the @CacheNamespaceRef");
            }
            if (refType != void.class && !refName.isEmpty()) {
                throw new BuilderException("Cannot use both value() and name() attribute in the @CacheNamespaceRef");
            }
            String namespace = refType != void.class ? refType.getName() : refName;
            try {
                assistant.useCacheRef(namespace);
            } catch (IncompleteElementException e) {
                configuration.addIncompleteCacheRef(new CacheRefResolver(assistant, namespace));
            }
        }
    }

    /**
     * 解析 ResultMap，方便后面解析statement时，应用该 ResultMap
     * 使用java的方式来描述 <constructor/> 和 <result /> 与 xml 的有点差别
     * 需要对每个方法进行描述，因为 mybatis 会将每个方法的配置，视作一个内联配置，而无法进行共用
     * @param method
     * @return
     */
    private String parseResultMap(Method method) {
        // 获取返回值
        Class<?> returnType = getReturnType(method, type);
        // 获取 Arg 注解列表，即用于 <constructor />
        Arg[] args = method.getAnnotationsByType(Arg.class);
        // 获取 Result，即 <result />
        Result[] results = method.getAnnotationsByType(Result.class);
        // 获取 TypeDiscriminator，即 <discriminator />
        TypeDiscriminator typeDiscriminator = method.getAnnotation(TypeDiscriminator.class);
        // 根据方法生成 resultMapId
        String resultMapId = generateResultMapName(method);
        applyResultMap(resultMapId, returnType, args, results, typeDiscriminator);
        return resultMapId;
    }

    /**
     * 根据方法生成 resultMapId
     * 如果指定id，则 class.id；
     * 如果未指定，则 class.method-param1-param2
     * @param method
     * @return
     */
    private String generateResultMapName(Method method) {
        // 获取 Results 注解，以及其 id 属性
        Results results = method.getAnnotation(Results.class);
        if (results != null && !results.id().isEmpty()) {
            // 如果指定了 id ，则拼接上 namespace
            return type.getName() + "." + results.id();
        }
        StringBuilder suffix = new StringBuilder();
        for (Class<?> c : method.getParameterTypes()) {
            suffix.append("-");
            suffix.append(c.getSimpleName());
        }
        if (suffix.length() < 1) {
            suffix.append("-void");
        }
        // 未指定id时，则 class.method-param1-param2
        return type.getName() + "." + method.getName() + suffix;
    }

    /**
     * 应用并注册 ResultMap
     * @param resultMapId
     * @param returnType
     * @param args
     * @param results
     * @param discriminator
     */
    private void applyResultMap(String resultMapId, Class<?> returnType, Arg[] args, Result[] results,
                                TypeDiscriminator discriminator) {
        List<ResultMapping> resultMappings = new ArrayList<>();
        applyConstructorArgs(args, returnType, resultMappings);
        applyResults(results, returnType, resultMappings);
        Discriminator disc = applyDiscriminator(resultMapId, returnType, discriminator);
        // 添加并注册 ResultMap
        assistant.addResultMap(resultMapId, returnType, null, disc, resultMappings, null);
        createDiscriminatorResultMaps(resultMapId, returnType, discriminator);
    }

    /**
     * 将 TypeDiscriminator 的每个 Case 转成一个特殊的 ResultMap
     * @param resultMapId
     * @param resultType
     * @param discriminator
     */
    private void createDiscriminatorResultMaps(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
        if (discriminator != null) {
            for (Case c : discriminator.cases()) {
                String caseResultMapId = resultMapId + "-" + c.value();
                List<ResultMapping> resultMappings = new ArrayList<>();
                // issue #136
                applyConstructorArgs(c.constructArgs(), resultType, resultMappings);
                applyResults(c.results(), resultType, resultMappings);
                // TODO add AutoMappingBehaviour
                assistant.addResultMap(caseResultMapId, c.type(), resultMapId, null, resultMappings, null);
            }
        }
    }

    /**
     * 应用 TypeDiscriminator，生成 Discriminator 对象
     * @param resultMapId
     * @param resultType
     * @param discriminator
     * @return
     */
    private Discriminator applyDiscriminator(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
        if (discriminator != null) {
            String column = discriminator.column();
            Class<?> javaType = discriminator.javaType() == void.class ? String.class : discriminator.javaType();
            JdbcType jdbcType = discriminator.jdbcType() == JdbcType.UNDEFINED ? null : discriminator.jdbcType();
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>) (discriminator
                    .typeHandler() == UnknownTypeHandler.class ? null : discriminator.typeHandler());
            Case[] cases = discriminator.cases();
            Map<String, String> discriminatorMap = new HashMap<>();
            for (Case c : cases) {
                String value = c.value();
                String caseResultMapId = resultMapId + "-" + value;
                discriminatorMap.put(value, caseResultMapId);
            }
            return assistant.buildDiscriminator(resultType, column, javaType, jdbcType, typeHandler, discriminatorMap);
        }
        return null;
    }

    /**
     * 解析 statement 对象
     * @param method
     */
    void parseStatement(Method method) {
        final Class<?> parameterTypeClass = getParameterType(method);
        final LanguageDriver languageDriver = getLanguageDriver(method);

        // 获取对应注解，进行值解析
        getAnnotationWrapper(method, true, statementAnnotationTypes).ifPresent(statementAnnotation -> {
            // 根据注解构建 SqlSource
            final SqlSource sqlSource = buildSqlSource(statementAnnotation.getAnnotation(), parameterTypeClass,
                    languageDriver, method);
            // 获取对应sql命令的类型，如：insert、update
            final SqlCommandType sqlCommandType = statementAnnotation.getSqlCommandType();
            // 命令附加选项，如 flushCache 等
            final Options options = getAnnotationWrapper(method, false, Options.class).map(x -> (Options) x.getAnnotation())
                    .orElse(null);
            // 生成 mappedStatement id
            final String mappedStatementId = type.getName() + "." + method.getName();

            final KeyGenerator keyGenerator;
            String keyProperty = null;
            String keyColumn = null;
            // INSERT或UPDATE则获取 SelectKey 注解
            if (SqlCommandType.INSERT.equals(sqlCommandType) || SqlCommandType.UPDATE.equals(sqlCommandType)) {
                // first check for SelectKey annotation - that overrides everything else
                SelectKey selectKey = getAnnotationWrapper(method, false, SelectKey.class)
                        .map(x -> (SelectKey) x.getAnnotation()).orElse(null);
                // 存在 SelectKey 则处理 SelectKey 注解
                if (selectKey != null) {
                    keyGenerator = handleSelectKeyAnnotation(selectKey, mappedStatementId, getParameterType(method),
                            languageDriver);
                    keyProperty = selectKey.keyProperty();
                } else if (options == null) { // 是否存在参数
                    keyGenerator = configuration.isUseGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
                } else {  // 使用普通（非自定义keyGenerator语句）
                    keyGenerator = options.useGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
                    keyProperty = options.keyProperty();
                    keyColumn = options.keyColumn();
                }
            } else {
                keyGenerator = NoKeyGenerator.INSTANCE;
            }

            Integer fetchSize = null;
            Integer timeout = null;
            StatementType statementType = StatementType.PREPARED;
            ResultSetType resultSetType = configuration.getDefaultResultSetType();
            boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
            boolean flushCache = !isSelect;
            boolean useCache = isSelect;
            // 初始化选项
            if (options != null) {
                if (FlushCachePolicy.TRUE.equals(options.flushCache())) {
                    flushCache = true;
                } else if (FlushCachePolicy.FALSE.equals(options.flushCache())) {
                    flushCache = false;
                }
                useCache = options.useCache();
                // issue #348
                fetchSize = options.fetchSize() > -1 || options.fetchSize() == Integer.MIN_VALUE ? options.fetchSize() : null;
                timeout = options.timeout() > -1 ? options.timeout() : null;
                statementType = options.statementType();
                if (options.resultSetType() != ResultSetType.DEFAULT) {
                    resultSetType = options.resultSetType();
                }
            }

            String resultMapId = null;
            // 如果是 select，则尝试获取 ResultMap 注解，如果指定了 ResultMap，则使用指定的ResultMap
            // 如果未指定，则使用 内联 的 ResultMap
            if (isSelect) {
                ResultMap resultMapAnnotation = method.getAnnotation(ResultMap.class);
                if (resultMapAnnotation != null) {
                    resultMapId = String.join(",", resultMapAnnotation.value());
                } else {
                    resultMapId = generateResultMapName(method);
                }
            }

            // 添加并注册 MappedStatement
            assistant.addMappedStatement(mappedStatementId, sqlSource, statementType, sqlCommandType, fetchSize, timeout,
                    // ParameterMapID
                    null, parameterTypeClass, resultMapId, getReturnType(method, type), resultSetType, flushCache, useCache,
                    // TODO gcode issue #577
                    false, keyGenerator, keyProperty, keyColumn, statementAnnotation.getDatabaseId(), languageDriver,
                    // ResultSets
                    options != null ? nullOrEmpty(options.resultSets()) : null, statementAnnotation.isDirtySelect());
        });
    }

    /**
     * 获取指定的 LanguageDriver 类型
     * @param method
     * @return
     */
    private LanguageDriver getLanguageDriver(Method method) {
        Lang lang = method.getAnnotation(Lang.class);
        Class<? extends LanguageDriver> langClass = null;
        if (lang != null) {
            langClass = lang.value();
        }
        return configuration.getLanguageDriver(langClass);
    }

    private Class<?> getParameterType(Method method) {
        Class<?> parameterType = null;
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> currentParameterType : parameterTypes) {
            if (!RowBounds.class.isAssignableFrom(currentParameterType)
                    && !ResultHandler.class.isAssignableFrom(currentParameterType)) {
                if (parameterType == null) {
                    parameterType = currentParameterType;
                } else {
                    // issue #135
                    parameterType = ParamMap.class;
                }
            }
        }
        return parameterType;
    }

    private static Class<?> getReturnType(Method method, Class<?> type) {
        Class<?> returnType = method.getReturnType();
        Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, type);
        if (resolvedReturnType instanceof Class) {
            returnType = (Class<?>) resolvedReturnType;
            if (returnType.isArray()) {
                returnType = returnType.getComponentType();
            }
            // gcode issue #508
            if (void.class.equals(returnType)) {
                ResultType rt = method.getAnnotation(ResultType.class);
                if (rt != null) {
                    returnType = rt.value();
                }
            }
        } else if (resolvedReturnType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) resolvedReturnType;
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            if (Collection.class.isAssignableFrom(rawType) || Cursor.class.isAssignableFrom(rawType)) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments != null && actualTypeArguments.length == 1) {
                    Type returnTypeParameter = actualTypeArguments[0];
                    if (returnTypeParameter instanceof Class<?>) {
                        returnType = (Class<?>) returnTypeParameter;
                    } else if (returnTypeParameter instanceof ParameterizedType) {
                        // (gcode issue #443) actual type can be a also a parameterized type
                        returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
                    } else if (returnTypeParameter instanceof GenericArrayType) {
                        Class<?> componentType = (Class<?>) ((GenericArrayType) returnTypeParameter).getGenericComponentType();
                        // (gcode issue #525) support List<byte[]>
                        returnType = Array.newInstance(componentType, 0).getClass();
                    }
                }
            } else if (method.isAnnotationPresent(MapKey.class) && Map.class.isAssignableFrom(rawType)) {
                // (gcode issue 504) Do not look into Maps if there is not MapKey annotation
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments != null && actualTypeArguments.length == 2) {
                    Type returnTypeParameter = actualTypeArguments[1];
                    if (returnTypeParameter instanceof Class<?>) {
                        returnType = (Class<?>) returnTypeParameter;
                    } else if (returnTypeParameter instanceof ParameterizedType) {
                        // (gcode issue 443) actual type can be a also a parameterized type
                        returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
                    }
                }
            } else if (Optional.class.equals(rawType)) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                Type returnTypeParameter = actualTypeArguments[0];
                if (returnTypeParameter instanceof Class<?>) {
                    returnType = (Class<?>) returnTypeParameter;
                }
            }
        }

        return returnType;
    }

    /**
     * 基于 Result 数组，生成 ResultMapping
     * @param results
     * @param resultType
     * @param resultMappings
     */
    private void applyResults(Result[] results, Class<?> resultType, List<ResultMapping> resultMappings) {
        for (Result result : results) {
            List<ResultFlag> flags = new ArrayList<>();
            if (result.id()) {
                flags.add(ResultFlag.ID);
            }
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>) (result
                    .typeHandler() == UnknownTypeHandler.class ? null : result.typeHandler());
            boolean hasNestedResultMap = hasNestedResultMap(result);
            ResultMapping resultMapping = assistant.buildResultMapping(resultType, nullOrEmpty(result.property()),
                    nullOrEmpty(result.column()), result.javaType() == void.class ? null : result.javaType(),
                    result.jdbcType() == JdbcType.UNDEFINED ? null : result.jdbcType(),
                    hasNestedSelect(result) ? nestedSelectId(result) : null,
                    hasNestedResultMap ? nestedResultMapId(result) : null, null,
                    hasNestedResultMap ? findColumnPrefix(result) : null, typeHandler, flags, null, null, isLazy(result));
            resultMappings.add(resultMapping);
        }
    }

    private String findColumnPrefix(Result result) {
        String columnPrefix = result.one().columnPrefix();
        if (columnPrefix.length() < 1) {
            columnPrefix = result.many().columnPrefix();
        }
        return columnPrefix;
    }

    private String nestedResultMapId(Result result) {
        String resultMapId = result.one().resultMap();
        if (resultMapId.length() < 1) {
            resultMapId = result.many().resultMap();
        }
        if (!resultMapId.contains(".")) {
            resultMapId = type.getName() + "." + resultMapId;
        }
        return resultMapId;
    }

    private boolean hasNestedResultMap(Result result) {
        if (result.one().resultMap().length() > 0 && result.many().resultMap().length() > 0) {
            throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
        }
        return result.one().resultMap().length() > 0 || result.many().resultMap().length() > 0;
    }

    private String nestedSelectId(Result result) {
        String nestedSelect = result.one().select();
        if (nestedSelect.length() < 1) {
            nestedSelect = result.many().select();
        }
        if (!nestedSelect.contains(".")) {
            nestedSelect = type.getName() + "." + nestedSelect;
        }
        return nestedSelect;
    }

    private boolean isLazy(Result result) {
        boolean isLazy = configuration.isLazyLoadingEnabled();
        if (result.one().select().length() > 0 && FetchType.DEFAULT != result.one().fetchType()) {
            isLazy = result.one().fetchType() == FetchType.LAZY;
        } else if (result.many().select().length() > 0 && FetchType.DEFAULT != result.many().fetchType()) {
            isLazy = result.many().fetchType() == FetchType.LAZY;
        }
        return isLazy;
    }

    private boolean hasNestedSelect(Result result) {
        if (result.one().select().length() > 0 && result.many().select().length() > 0) {
            throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
        }
        return result.one().select().length() > 0 || result.many().select().length() > 0;
    }

    /**
     * 基于 Arg 注解以及 方法返回值，构建生成 constructor 的 ResultMapping
     * @param args
     * @param resultType
     * @param resultMappings
     */
    private void applyConstructorArgs(Arg[] args, Class<?> resultType, List<ResultMapping> resultMappings) {
        for (Arg arg : args) {
            List<ResultFlag> flags = new ArrayList<>();
            flags.add(ResultFlag.CONSTRUCTOR);
            if (arg.id()) {
                flags.add(ResultFlag.ID);
            }
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>) (arg
                    .typeHandler() == UnknownTypeHandler.class ? null : arg.typeHandler());
            ResultMapping resultMapping = assistant.buildResultMapping(resultType, nullOrEmpty(arg.name()),
                    nullOrEmpty(arg.column()), arg.javaType() == void.class ? null : arg.javaType(),
                    arg.jdbcType() == JdbcType.UNDEFINED ? null : arg.jdbcType(), nullOrEmpty(arg.select()),
                    nullOrEmpty(arg.resultMap()), null, nullOrEmpty(arg.columnPrefix()), typeHandler, flags, null, null, false);
            resultMappings.add(resultMapping);
        }
    }

    private String nullOrEmpty(String value) {
        return value == null || value.trim().length() == 0 ? null : value;
    }

    private KeyGenerator handleSelectKeyAnnotation(SelectKey selectKeyAnnotation, String baseStatementId,
                                                   Class<?> parameterTypeClass, LanguageDriver languageDriver) {
        String id = baseStatementId + SelectKeyGenerator.SELECT_KEY_SUFFIX;
        Class<?> resultTypeClass = selectKeyAnnotation.resultType();
        StatementType statementType = selectKeyAnnotation.statementType();
        String keyProperty = selectKeyAnnotation.keyProperty();
        String keyColumn = selectKeyAnnotation.keyColumn();
        boolean executeBefore = selectKeyAnnotation.before();

        // defaults
        boolean useCache = false;
        KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
        Integer fetchSize = null;
        Integer timeout = null;
        boolean flushCache = false;
        String parameterMap = null;
        String resultMap = null;
        ResultSetType resultSetTypeEnum = null;
        String databaseId = selectKeyAnnotation.databaseId().isEmpty() ? null : selectKeyAnnotation.databaseId();

        SqlSource sqlSource = buildSqlSource(selectKeyAnnotation, parameterTypeClass, languageDriver, null);
        SqlCommandType sqlCommandType = SqlCommandType.SELECT;

        assistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType, fetchSize, timeout, parameterMap,
                parameterTypeClass, resultMap, resultTypeClass, resultSetTypeEnum, flushCache, useCache, false, keyGenerator,
                keyProperty, keyColumn, databaseId, languageDriver, null, false);

        id = assistant.applyCurrentNamespace(id, false);

        MappedStatement keyStatement = configuration.getMappedStatement(id, false);
        SelectKeyGenerator answer = new SelectKeyGenerator(keyStatement, executeBefore);
        configuration.addKeyGenerator(id, answer);
        return answer;
    }

    private SqlSource buildSqlSource(Annotation annotation, Class<?> parameterType, LanguageDriver languageDriver,
                                     Method method) {
        if (annotation instanceof Select) {
            return buildSqlSourceFromStrings(((Select) annotation).value(), parameterType, languageDriver);
        }
        if (annotation instanceof Update) {
            return buildSqlSourceFromStrings(((Update) annotation).value(), parameterType, languageDriver);
        } else if (annotation instanceof Insert) {
            return buildSqlSourceFromStrings(((Insert) annotation).value(), parameterType, languageDriver);
        } else if (annotation instanceof Delete) {
            return buildSqlSourceFromStrings(((Delete) annotation).value(), parameterType, languageDriver);
        } else if (annotation instanceof SelectKey) {
            return buildSqlSourceFromStrings(((SelectKey) annotation).statement(), parameterType, languageDriver);
        }
        return new ProviderSqlSource(assistant.getConfiguration(), annotation, type, method);
    }

    private SqlSource buildSqlSourceFromStrings(String[] strings, Class<?> parameterTypeClass,
                                                LanguageDriver languageDriver) {
        return languageDriver.createSqlSource(configuration, String.join(" ", strings).trim(), parameterTypeClass);
    }

    /**
     * 获取注解包装对象
     * @param method
     * @param errorIfNoMatch 是否没匹配中时抛出异常
     * @param targetTypes 需要找的注解
     * @return
     */
    @SafeVarargs
    private final Optional<AnnotationWrapper> getAnnotationWrapper(Method method, boolean errorIfNoMatch,
                                                                   Class<? extends Annotation>... targetTypes) {
        return getAnnotationWrapper(method, errorIfNoMatch, Arrays.asList(targetTypes));
    }

    /**
     * 找到对应注解，并解析包装
     * @param method
     * @param errorIfNoMatch
     * @param targetTypes
     * @return
     */
    private Optional<AnnotationWrapper> getAnnotationWrapper(Method method, boolean errorIfNoMatch,
                                                             Collection<Class<? extends Annotation>> targetTypes) {
        String databaseId = configuration.getDatabaseId();
        // 获取对应注解，并使用 databaseId 映射
        Map<String, AnnotationWrapper> statementAnnotations = targetTypes.stream()
                .flatMap(x -> Arrays.stream(method.getAnnotationsByType(x))).map(AnnotationWrapper::new)
                .collect(Collectors.toMap(AnnotationWrapper::getDatabaseId, x -> x, (existing, duplicate) -> {
                    throw new BuilderException(
                            String.format("Detected conflicting annotations '%s' and '%s' on '%s'.", existing.getAnnotation(),
                                    duplicate.getAnnotation(), method.getDeclaringClass().getName() + "." + method.getName()));
                }));

        AnnotationWrapper annotationWrapper = null;
        // 获取指定 databaseId 的注解方法
        if (databaseId != null) {
            annotationWrapper = statementAnnotations.get(databaseId);
        }
        // 如果未指定，则获取未指定注解方法
        if (annotationWrapper == null) {
            annotationWrapper = statementAnnotations.get("");
        }
        // 如果未找到当前databaseId对应的标注方法，并且需要抛出异常则抛出
        if (errorIfNoMatch && annotationWrapper == null && !statementAnnotations.isEmpty()) {
            // Annotations exist, but there is no matching one for the specified databaseId
            throw new BuilderException(String.format(
                    "Could not find a statement annotation that correspond a current database or default statement on method '%s.%s'. Current database id is [%s].",
                    method.getDeclaringClass().getName(), method.getName(), databaseId));
        }
        return Optional.ofNullable(annotationWrapper);
    }

    /**
     * 根据namespace、id获取到对应方法的返回类型
     *
     * @param mapperFqn
     * @param localStatementId
     * @return
     */
    public static Class<?> getMethodReturnType(String mapperFqn, String localStatementId) {
        if (mapperFqn == null || localStatementId == null) {
            return null;
        }
        try {
            Class<?> mapperClass = Resources.classForName(mapperFqn);
            for (Method method : mapperClass.getMethods()) {
                if (method.getName().equals(localStatementId) && canHaveStatement(method)) {
                    return getReturnType(method, mapperClass);
                }
            }
        } catch (ClassNotFoundException e) {
            // No corresponding mapper interface which is OK
        }
        return null;
    }

    /**
     * annotation 包装类
     */
    private static class AnnotationWrapper {
        private final Annotation annotation;
        private final String databaseId;
        private final SqlCommandType sqlCommandType;
        private boolean dirtySelect;

        AnnotationWrapper(Annotation annotation) {
            this.annotation = annotation;
            if (annotation instanceof Select) {
                databaseId = ((Select) annotation).databaseId();
                sqlCommandType = SqlCommandType.SELECT;
                dirtySelect = ((Select) annotation).affectData();
            } else if (annotation instanceof Update) {
                databaseId = ((Update) annotation).databaseId();
                sqlCommandType = SqlCommandType.UPDATE;
            } else if (annotation instanceof Insert) {
                databaseId = ((Insert) annotation).databaseId();
                sqlCommandType = SqlCommandType.INSERT;
            } else if (annotation instanceof Delete) {
                databaseId = ((Delete) annotation).databaseId();
                sqlCommandType = SqlCommandType.DELETE;
            } else if (annotation instanceof SelectProvider) {
                databaseId = ((SelectProvider) annotation).databaseId();
                sqlCommandType = SqlCommandType.SELECT;
                dirtySelect = ((SelectProvider) annotation).affectData();
            } else if (annotation instanceof UpdateProvider) {
                databaseId = ((UpdateProvider) annotation).databaseId();
                sqlCommandType = SqlCommandType.UPDATE;
            } else if (annotation instanceof InsertProvider) {
                databaseId = ((InsertProvider) annotation).databaseId();
                sqlCommandType = SqlCommandType.INSERT;
            } else if (annotation instanceof DeleteProvider) {
                databaseId = ((DeleteProvider) annotation).databaseId();
                sqlCommandType = SqlCommandType.DELETE;
            } else {
                sqlCommandType = SqlCommandType.UNKNOWN;
                if (annotation instanceof Options) {
                    databaseId = ((Options) annotation).databaseId();
                } else if (annotation instanceof SelectKey) {
                    databaseId = ((SelectKey) annotation).databaseId();
                } else {
                    databaseId = "";
                }
            }
        }

        Annotation getAnnotation() {
            return annotation;
        }

        SqlCommandType getSqlCommandType() {
            return sqlCommandType;
        }

        String getDatabaseId() {
            return databaseId;
        }

        boolean isDirtySelect() {
            return dirtySelect;
        }
    }
}
