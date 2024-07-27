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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.*;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;

/**
 * 基于xml配置的 mapper 建造器。必须知道：
 * 1.ParameterMapping：实际上就是 mybatis 对 入参 及 sql 进行解析后，用于最终  PreparedStatement 填充值的映射描述信息
 * 2.ResultMap：在 <resultMap> 中，可能会嵌套 <association>、<collection>，而这些标签实际上结构和 resultMap 类似，因此mybatis在处理时会将其解析为 ResultMap，然后通过 id 引用的方式进行关联
 * 3.ReusltMapping：对于 <resultMap> 内的所有子标签（如 association、collection、id、result、以及 discriminator 里的 case），都会被处理成 ResultMapping
 * 4.每次调用 resultMapElement 方法，都会往 Configuration.resultMaps 里把 ResultMap 注册上去。
 * 例如 <resultMap> 标签解析，以及对应子标签 association、collection、constructor、discriminator 等都会调用该方法（即都会注册到 Configuration 中）
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

    private final XPathParser parser;
    private final MapperBuilderAssistant builderAssistant;
    private final Map<String, XNode> sqlFragments;
    private final String resource;

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments,
                            String namespace) {
        this(reader, configuration, resource, sqlFragments);
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments) {
        this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()), configuration,
                resource, sqlFragments);
    }

    /**
     * 构建一个提供了命名空间的建造器，用于兼容通过mapper接口构建mapper
     *
     * @param inputStream
     * @param configuration
     * @param resource
     * @param sqlFragments
     * @param namespace
     */
    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments, String namespace) {
        this(inputStream, configuration, resource, sqlFragments);
        // 预先设置当前的命名空间
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments) {
        this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()), configuration,
                resource, sqlFragments);
    }

    private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource,
                             Map<String, XNode> sqlFragments) {
        super(configuration);
        // 基于 configuration 、 resource 构建一个 mapper 建造协助器
        this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
        this.parser = parser;
        this.sqlFragments = sqlFragments;
        this.resource = resource;
    }

    /**
     * 从xml中解析mapper信息
     */
    public void parse() {
        // 判断资源是否加载
        if (!configuration.isResourceLoaded(resource)) {
            // 解析mapper标签，并加载对于 mapper.xml 配置
            configurationElement(parser.evalNode("/mapper"));
            // 添加到已加载的资源列表中
            configuration.addLoadedResource(resource);
            // 将命名空间对应的mapper接口注册到configuration中
            bindMapperForNamespace();
        }
        // 当所有资源都加载完成后，开始处理 incomplete 的数据
        configuration.parsePendingResultMaps(false);
        configuration.parsePendingCacheRefs(false);
        configuration.parsePendingStatements(false);
    }

    public XNode getSqlFragment(String refid) {
        return sqlFragments.get(refid);
    }

    private void configurationElement(XNode context) {
        try {
            // 获取命名空间
            String namespace = context.getStringAttribute("namespace");
            if (namespace == null || namespace.isEmpty()) {
                throw new BuilderException("Mapper's namespace cannot be empty");
            }
            // 通过 建造协助器 设置当前命名空间。
            // 如果是通过 mapper 接口时，会顺便校验xml的namespace是否与原先设置的mapper接口全类名一直
            builderAssistant.setCurrentNamespace(namespace);
            // 引用其它命名空间的缓存配置，两个命名空间的操作使用的是同一个Cache，实际上就是与其他mapper共用同一个二级缓存
            // *** 使用这个时，需要 select 的返回类型对象都需要实现 Serializable ***
            cacheRefElement(context.evalNode("cache-ref"));
            // 二级缓存解析，解析 <cache> 标签配置
            cacheElement(context.evalNode("cache"));
            // 处理解析 parameterMap 标签
            parameterMapElement(context.evalNodes("/mapper/parameterMap"));
            // 处理解析 resultMap 标签
            resultMapElements(context.evalNodes("/mapper/resultMap"));
            // 处理 sql 代码块标签
            sqlElement(context.evalNodes("/mapper/sql"));
            // 通过 select/insert/update/delete 标签构建 statement 语句
            buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
        } catch (Exception e) {
            throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e, e);
        }
    }

    /**
     * ***************** 重点
     * 开始解析 select、insert、update、delete 标签
     * @param list
     */
    private void buildStatementFromContext(List<XNode> list) {
        // 同sql标签，如果Configuration中指定了databaseId，则只加载对应databaseId的sql语句
        if (configuration.getDatabaseId() != null) {
            buildStatementFromContext(list, configuration.getDatabaseId());
        }
        buildStatementFromContext(list, null);
    }

    /**
     * 加载指定 databaseId 的语句
     * @param list
     * @param requiredDatabaseId
     */
    private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
        for (XNode context : list) {
            final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant, context,
                    requiredDatabaseId);
            try {
                // 开始解析 statement
                statementParser.parseStatementNode();
            } catch (IncompleteElementException e) {
                // 如果解析statement失败，则加入未完成的队列中，等所有初始化后在进行初始化
                configuration.addIncompleteStatement(statementParser);
            }
        }
    }

    private void cacheRefElement(XNode context) {
        if (context != null) {
            // 添加缓存引用映射
            configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));
            // 创建缓存引用对象
            CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant,
                    context.getStringAttribute("namespace"));
            try {
                // 解析缓存引用关系，实际委托给 MapperBuilderAssistant.useCacheRef(cacheRefNamespace) 进行处理
                cacheRefResolver.resolveCacheRef();
            } catch (IncompleteElementException e) {
                // 如果找不到 cache-ref 对应的 namespace，则表示还未加载到，将其加入未完成的缓存引用集中
                configuration.addIncompleteCacheRef(cacheRefResolver);
            }
        }
    }

    private void cacheElement(XNode context) {
        if (context != null) {
            // 加载缓存类型，支持自定义类型，注意，基本上cache的参数，都只有在 type 为 PERPETUAL 才会生效！！
            String type = context.getStringAttribute("type", "PERPETUAL");
            Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
            // 淘汰策略，支持自定义策略
            String eviction = context.getStringAttribute("eviction", "LRU");
            Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
            // 清空缓存数据的间隔
            Long flushInterval = context.getLongAttribute("flushInterval");
            // 缓存大小
            Integer size = context.getIntAttribute("size");
            // 是否仅读
            boolean readWrite = !context.getBooleanAttribute("readOnly", false);
            // 是否支持阻塞
            boolean blocking = context.getBooleanAttribute("blocking", false);
            // 自定义属性
            Properties props = context.getChildrenAsProperties();
            // 创建并注册缓存对象
            builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
        }
    }

    /**
     * 解析 parameterMap 标签解析成 List<ParameterMapping>
     * parameterMap实际上内部包含多个 ParameterMapping，
     * 而ParameterMapping则是 mybatis 对 入参及sql进行解析后，用于最后 PreparedStatement 填充值的映射描述信息
     *
     * 使用parameterMap，能够直接跳过 mybatis 对入参的解析，直接声明最终的映射描述信息，但这种方法不灵活，较少使用
     * @param list
     */
    private void parameterMapElement(List<XNode> list) {
        for (XNode parameterMapNode : list) {
            String id = parameterMapNode.getStringAttribute("id");
            String type = parameterMapNode.getStringAttribute("type");
            Class<?> parameterClass = resolveClass(type);
            List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
            List<ParameterMapping> parameterMappings = new ArrayList<>();
            for (XNode parameterNode : parameterNodes) {
                String property = parameterNode.getStringAttribute("property");
                String javaType = parameterNode.getStringAttribute("javaType");
                String jdbcType = parameterNode.getStringAttribute("jdbcType");
                String resultMap = parameterNode.getStringAttribute("resultMap");
                String mode = parameterNode.getStringAttribute("mode");
                String typeHandler = parameterNode.getStringAttribute("typeHandler");
                Integer numericScale = parameterNode.getIntAttribute("numericScale");
                ParameterMode modeEnum = resolveParameterMode(mode);
                Class<?> javaTypeClass = resolveClass(javaType);
                JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
                Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
                ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property,
                        javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
                parameterMappings.add(parameterMapping);
            }
            // 保存 ParameterMap 解析后的结果
            builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
        }
    }

    /**
     * 解析所有的resultMap
     * @param list
     */
    private void resultMapElements(List<XNode> list) {
        for (XNode resultMapNode : list) {
            try {
                // 逐个解析 resultMap 标签
                resultMapElement(resultMapNode);
            } catch (IncompleteElementException e) {
                // ignore, it will be retried
            }
        }
    }

    /**
     * 解析 resultMap
     */
    private ResultMap resultMapElement(XNode resultMapNode) {
        return resultMapElement(resultMapNode, Collections.emptyList(), null);
    }

    /**
     * 进行解析并注册resultMap标签，
     * 实际上该方法不止<resultMap />会调用，包括resultMap的子标签<discriminator />、<collection />、<constructor />和<association /> 都会通过该方法来解析。
     * Mybatis会把这几个标签单程特殊的 resultMap 来处理，只是使用 flag或其他的方式来进行标记
     * @param resultMapNode
     * @param additionalResultMappings
     * @param enclosingType
     * @return
     */
    private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings,
                                       Class<?> enclosingType) {
        ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());
        // 获取对应的java类型
        String type = resultMapNode.getStringAttribute("type", resultMapNode.getStringAttribute("ofType",
                resultMapNode.getStringAttribute("resultType", resultMapNode.getStringAttribute("javaType"))));
        Class<?> typeClass = resolveClass(type);
        if (typeClass == null) {
            // 如果未直接定义类型，则会尝试从 标签中的属性 获取 对应类型
            // 如 association，则会根据 property 从父节点的类型中找到对应属性并获取类型
            typeClass = inheritEnclosingType(resultMapNode, enclosingType);
        }
        Discriminator discriminator = null;
        // 记录每个属性结果集映射
        List<ResultMapping> resultMappings = new ArrayList<>(additionalResultMappings);
        List<XNode> resultChildren = resultMapNode.getChildren();
        // 对 resultMap 中的 子标签 进行解析处理
        for (XNode resultChild : resultChildren) {
            if ("constructor".equals(resultChild.getName())) { // <constructor /> 处理
                processConstructorElement(resultChild, typeClass, resultMappings);
            } else if ("discriminator".equals(resultChild.getName())) { // <discriminator /> 处理
                discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
            } else { // 对 id、result、association、collection 等标签处理
                List<ResultFlag> flags = new ArrayList<>();
                if ("id".equals(resultChild.getName())) { // 如果是id还需加上对应的标识
                    flags.add(ResultFlag.ID);
                }
                // 开始构建一个 ResultMapping
                resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
            }
        }
        String id = resultMapNode.getStringAttribute("id", // 获取id
                // 如果没id时，则是 resultMap 子标签的情况，这种属于 内部特殊resultMap，因此自动生成id
                // 规则：父标签名称_标签名称[id或name]；如：mapper_resultMap[User]_discriminator_case[123]
                resultMapNode.getValueBasedIdentifier());
        // 获取父resultMap
        String extend = resultMapNode.getStringAttribute("extends");
        // 是否开启自动映射
        Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
        // 使用已解析的ResultMapping集，创建对应的ResultMapResolver解析器，从而解析到 ResultMap
        ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend, discriminator,
                resultMappings, autoMapping);
        try {
            // 最终解析，实际上委托给了 MapperBuilderAssistant 的 addResultMap
            // 该方法也会往 Configuration 中注册当前 ResultMap 对象
            return resultMapResolver.resolve();
        } catch (IncompleteElementException e) {
            // 如果出现解析失败的异常，说明存在的依赖 resultMap 为初始化完成，则将其加入未完成的 ResultMap 列表中
            configuration.addIncompleteResultMap(resultMapResolver);
            throw e;
        }
    }

    /**
     * 获取 标签内部类型。如 association，则会根据 property 从父节点的类型中找到对应属性并获取类型
     * @param enclosingType 如果是case的节点时，其类型实际上是从父 resultMap 中获取 case 对应的 property 的类型，因此需要通过该参数来传入类型
     *                      <resultMap id="demo" javaType="Demo"><case property="user"> ... </case></resultMap>，其 enclosingType 为 Demo#user 的类型
     * @return
     */
    protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {
        // 如果是association，并且未指定 resultMap，则获取对应属性的类型
        if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
            String property = resultMapNode.getStringAttribute("property");
            if (property != null && enclosingType != null) {
                MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
                return metaResultType.getSetterType(property);
            }
        } else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
            return enclosingType;
        }
        return null;
    }

    /**
     * 处理 constructor 构造器节点
     */
    private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings) {
        List<XNode> argChildren = resultChild.getChildren();
        // 遍历constructor所有子标签
        for (XNode argChild : argChildren) {
            List<ResultFlag> flags = new ArrayList<>();
            flags.add(ResultFlag.CONSTRUCTOR); // 标记为构造器的ResultMapping
            if ("idArg".equals(argChild.getName())) { // 如果是 idArg，则追加 id 的标记
                flags.add(ResultFlag.ID);
            }
            // 构建并添加mapping
            resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
        }
    }

    /**
     * 处理 discriminator，会将其每个子标签 <case/> 视为一个独立的 resultMap，然后通过 value - caseResultMap 的方式将其关联起来
     * @param context
     * @param resultType
     * @param resultMappings
     * @return
     */
    private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType,
                                                      List<ResultMapping> resultMappings) {
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        String typeHandler = context.getStringAttribute("typeHandler");
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Map<String, String> discriminatorMap = new HashMap<>();
        for (XNode caseChild : context.getChildren()) {
            // 获取 value 值
            String value = caseChild.getStringAttribute("value");
            // 如果是直接引用其他 resultMap 的方式，则直接获取
            // 如果不是，则将当前 case 节点，当成 resultMap 进行解析，得到对应的 id
            String resultMap = caseChild.getStringAttribute("resultMap",
                    processNestedResultMappings(caseChild, resultMappings, resultType));
            // 添加 value - resultMapId
            discriminatorMap.put(value, resultMap);
        }
        // 构建 Discriminator
        return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass,
                discriminatorMap);
    }

    /**
     * 处理 sql 标签，该方法并不会对sql进行实际解析，而是将所有的sql标签片段进行登记保存
     * @param list
     */
    private void sqlElement(List<XNode> list) {
        // 如果指定当前数据库标识，则加载sql标签时，需要加上数据库标识的过滤
        if (configuration.getDatabaseId() != null) {
            sqlElement(list, configuration.getDatabaseId());
        }
        sqlElement(list, null);
    }

    /**
     * 解析指定数据库标识的sql元素
     * @param list
     * @param requiredDatabaseId
     */
    private void sqlElement(List<XNode> list, String requiredDatabaseId) {
        for (XNode context : list) {
            // 获取当前标签生效的数据库标识
            String databaseId = context.getStringAttribute("databaseId");
            String id = context.getStringAttribute("id");
            // 判断id有效性，并校验是否符合当前数据库标识
            id = builderAssistant.applyCurrentNamespace(id, false);
            if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
                // 保存当前的sql判断
                sqlFragments.put(id, context);
            }
        }
    }

    /**
     * 判断指定的数据库标识是否一致
     * @param id
     * @param databaseId 当前元素的数据库标识
     * @param requiredDatabaseId 所需的数据库标识
     * @return
     */
    private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
        if (requiredDatabaseId != null) {
            return requiredDatabaseId.equals(databaseId);
        }
        if (databaseId != null) {
            return false;
        }
        // 如果当前sql未被解析，则直接标识需要加载
        if (!this.sqlFragments.containsKey(id)) {
            return true;
        }
        // skip this fragment if there is a previous one with a not null databaseId
        // 如果已被解析过，则使用旧标识的值来进行判断
        XNode context = this.sqlFragments.get(id);
        return context.getStringAttribute("databaseId") == null;
    }

    /**
     * 构建基础的 ResultMapping
     */
    private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) {
        String property;
        // 不同用途的 ResultMapping，配置属性名的 attr 不同
        if (flags.contains(ResultFlag.CONSTRUCTOR)) {
            property = context.getStringAttribute("name");
        } else {
            property = context.getStringAttribute("property");
        }
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        // 获取是否使用嵌套sql的方式装配数据
        String nestedSelect = context.getStringAttribute("select");
        // 获取具体字段的resultMap类型，由于 association、collection 等子标签，可能直接引用已存在的 resultMap 作为类型描述，因此这里直接获取目标嵌套的 resultMap。
        String nestedResultMap = context.getStringAttribute("resultMap",
                // 如果 resultMap 属性不存在时，那么将会把当前节点，当作特殊的 resultMap 进行解析，因为 association、collection 在不使用 resultMap 时，能够自定义属性映射逻辑，
                // 如：<collection property="address"> <result property="city" column="city" /> <result property="province" column="province" /> </collection>
                // 这时的解析结果，会被视为一个嵌套
                () -> processNestedResultMappings(context, Collections.emptyList(), resultType));
        String notNullColumn = context.getStringAttribute("notNullColumn");
        String columnPrefix = context.getStringAttribute("columnPrefix");
        String typeHandler = context.getStringAttribute("typeHandler");
        // 获取当前结果映射的数据来源是从哪个名称的结果集中获取的（要求与select时resultSets指定的名称一致）
        String resultSet = context.getStringAttribute("resultSet");
        String foreignColumn = context.getStringAttribute("foreignColumn");
        boolean lazy = "lazy"
                .equals(context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum, nestedSelect,
                nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet, foreignColumn, lazy);
    }

    /**
     * 处理嵌套的result集，这个方法实际上会为 association、collection、case 的标签描述信息 与 传入的外部resultMappings
     * 来构建一个新的 ResultMap，也就是说会为 discriminator 中的每个 case 构建一个 ResultMap
     * @param context
     * @param resultMappings
     * @param enclosingType
     * @return
     */
    private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings,
                                               Class<?> enclosingType) {
        // 处理 association、collection、case 节点，并且不使用嵌套查询sql的方式，那么将会把当前节点视为一个 特殊的resultMap 节点
        // 如果用了嵌套select，那么将使用对应select语句的类型作为该属性类型，也就不需要对子属性进行解析了
        if (Arrays.asList("association", "collection", "case").contains(context.getName())
                && context.getStringAttribute("select") == null) {
            validateCollection(context, enclosingType);
            // 将该节点视作 resultMap 进行解析并注册
            // 注意这里，会传入 discriminator 所在 ResultMap 的 ResultMapping 列表，作为附加映射项，加到 discriminator 解析后的 ResultMap 中
            ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
            // 获取其 resultMap 的 id
            return resultMap.getId();
        }
        return null;
    }

    protected void validateCollection(XNode context, Class<?> enclosingType) {
        // 校验 collection 节点，如果是collection节点，并且未设置集合类型时，则需要检查是否能找到对应的属性
        if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
                && context.getStringAttribute("javaType") == null) {
            MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
            String property = context.getStringAttribute("property");
            if (!metaResultType.hasSetter(property)) {
                throw new BuilderException(
                        "Ambiguous collection type for property '" + property + "'. You must specify 'javaType' or 'resultMap'.");
            }
        }
    }

    /**
     * 将 namespace对应的Mapper接口注册到 configuration 中
     */
    private void bindMapperForNamespace() {
        String namespace = builderAssistant.getCurrentNamespace();
        if (namespace != null) {
            Class<?> boundType = null;
            try {
                boundType = Resources.classForName(namespace);
            } catch (ClassNotFoundException e) {
                // ignore, bound type is not required
            }
            // 如果找到了对
            if (boundType != null && !configuration.hasMapper(boundType)) {
                // Spring may not know the real resource name so we set a flag
                // to prevent loading again this resource from the mapper interface
                // look at MapperAnnotationBuilder#loadXmlResource
                // 声明已经加载的数据
                configuration.addLoadedResource("namespace:" + namespace);
                // 将mapper接口注册
                configuration.addMapper(boundType);
            }
        }
    }

}
