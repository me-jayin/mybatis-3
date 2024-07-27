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

import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * StaticSqlSource 构建器，构建一个 StaticSqlSource 时，会解析 sql 语句中的 #{} 占位符，将其替换为 ? 并记录参数映射列表
 * @author Clinton Begin
 */
public class SqlSourceBuilder extends BaseBuilder {

    private static final String PARAMETER_PROPERTIES = "javaType,jdbcType,mode,numericScale,resultMap,typeHandler,jdbcTypeName";

    public SqlSourceBuilder(Configuration configuration) {
        super(configuration);
    }

    /**
     * 开始解析sql并构建 StaticSqlSource，这里会解析所有的 #{} 占位符，将其替换为 ?，并且记录每个占位符与参数的映射关系
     *
     * @param originalSql
     * @param parameterType
     * @param additionalParameters
     * @return
     */
    public SqlSource parse(String originalSql, Class<?> parameterType, Map<String, Object> additionalParameters) {
        // 进行记录 ParameterMapping 的 TokenHandler，在解析到 #{} 占位符时，会将占位符替换为 ? ，并通过 ParameterMapping 记录取值路径
        // 重点：org.apache.ibatis.builder.SqlSourceBuilder.ParameterMappingTokenHandler.handleToken
        ParameterMappingTokenHandler handler = new ParameterMappingTokenHandler(configuration, parameterType,
                additionalParameters);
        GenericTokenParser parser = new GenericTokenParser("#{", "}", handler);
        String sql;
        // 是否需要压缩空格
        if (configuration.isShrinkWhitespacesInSql()) {
            sql = parser.parse(removeExtraWhitespaces(originalSql));
        } else {
            // 开始解析 sql，替换占位符并生成 ParameterMapping
            sql = parser.parse(originalSql);
        }
        // 获取解析后得到的 ParameterMapping，并构建 StaticSqlSource
        return new StaticSqlSource(configuration, sql, handler.getParameterMappings());
    }

    /**
     * 移除多余空格
     *
     * @param original
     * @return
     */
    public static String removeExtraWhitespaces(String original) {
        // jdk的分隔符工具类，默认分格符" \t\n\r\f"
        StringTokenizer tokenizer = new StringTokenizer(original);
        StringBuilder builder = new StringBuilder();
        // 判断是否存在切割后的token
        boolean hasMoreTokens = tokenizer.hasMoreTokens();
        while (hasMoreTokens) {
            builder.append(tokenizer.nextToken());
            hasMoreTokens = tokenizer.hasMoreTokens();
            if (hasMoreTokens) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }

    /**
     * ParameterMapping 的 TokenHandler 处理器，该处理器处理表达时时，会表达式替换为 ? ，并把当前表达式按顺序添加到 ParameterMapping 列表中。
     * 保证后续解析 PreparedStatement 的值填充时能够找到对应的 value
     */
    private static class ParameterMappingTokenHandler extends BaseBuilder implements TokenHandler {

        private final List<ParameterMapping> parameterMappings = new ArrayList<>();
        private final Class<?> parameterType;
        private final MetaObject metaParameters;

        public ParameterMappingTokenHandler(Configuration configuration, Class<?> parameterType,
                                            Map<String, Object> additionalParameters) {
            super(configuration);
            this.parameterType = parameterType;
            this.metaParameters = configuration.newMetaObject(additionalParameters);
        }

        public List<ParameterMapping> getParameterMappings() {
            return parameterMappings;
        }

        /**
         * 该方法会在解析占位符时，将其替换为 ? ，并根据取值路径生成 ParameterMapping 列表
         *
         * @param content
         * @return
         */
        @Override
        public String handleToken(String content) {
            // 遇到占位符时，构建并记录 ParameterMapping
            parameterMappings.add(buildParameterMapping(content));
            // 将占位符表达式替换为 ?
            return "?";
        }

        /**
         * 构建一个 ParameterMapping
         *
         * @param content
         * @return
         */
        private ParameterMapping buildParameterMapping(String content) {
            // 解析表达式结果
            Map<String, String> propertiesMap = parseParameterMapping(content);
            // 获取属性取值表达式
            String property = propertiesMap.get("property");
            Class<?> propertyType;
            // 判断是否有get操作
            if (metaParameters.hasGetter(property)) { // issue #448 get type from additional params
                propertyType = metaParameters.getGetterType(property);
            } else if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
                propertyType = parameterType;
            } else if (JdbcType.CURSOR.name().equals(propertiesMap.get("jdbcType"))) {
                propertyType = java.sql.ResultSet.class;
            } else if (property == null || Map.class.isAssignableFrom(parameterType)) { // 对 map 的兜底
                propertyType = Object.class;
            } else {
                // 获取指定属性的 类型
                MetaClass metaClass = MetaClass.forClass(parameterType, configuration.getReflectorFactory());
                if (metaClass.hasGetter(property)) {
                    propertyType = metaClass.getGetterType(property);
                } else {
                    propertyType = Object.class;
                }
            }
            // 构建一个 ParameterMapping 的构建者，这里将 properties 进行设置
            ParameterMapping.Builder builder = new ParameterMapping.Builder(configuration, property, propertyType);
            Class<?> javaType = propertyType;
            String typeHandlerAlias = null;
            // 对 #{} 的属性设置进行初始化，如 javaType 等附加属性
            for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
                String name = entry.getKey();
                String value = entry.getValue();
                if ("javaType".equals(name)) {
                    javaType = resolveClass(value);
                    builder.javaType(javaType);
                } else if ("jdbcType".equals(name)) {
                    builder.jdbcType(resolveJdbcType(value));
                } else if ("mode".equals(name)) {
                    builder.mode(resolveParameterMode(value));
                } else if ("numericScale".equals(name)) {
                    builder.numericScale(Integer.valueOf(value));
                } else if ("resultMap".equals(name)) {
                    builder.resultMapId(value);
                } else if ("typeHandler".equals(name)) {
                    typeHandlerAlias = value;
                } else if ("jdbcTypeName".equals(name)) {
                    builder.jdbcTypeName(value);
                } else if ("property".equals(name)) { // 由于构建builder时已经设置该值，因此这里忽略
                    // Do Nothing
                } else if ("expression".equals(name)) {
                    throw new BuilderException("Expression based parameters are not supported yet");
                } else {
                    throw new BuilderException("An invalid property '" + name + "' was found in mapping #{" + content
                            + "}.  Valid properties are " + PARAMETER_PROPERTIES);
                }
            }
            if (typeHandlerAlias != null) {
                builder.typeHandler(resolveTypeHandler(javaType, typeHandlerAlias));
            }
            return builder.build();
        }

        /**
         * 基于表达式，解析出取值路径、附加参数等，如 address.city, jdbc=xx
         *
         * @param content
         * @return
         */
        private Map<String, String> parseParameterMapping(String content) {
            try {
                return new ParameterExpression(content);
            } catch (BuilderException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new BuilderException("Parsing error was found in mapping #{" + content
                        + "}.  Check syntax #{property|(expression), var1=value1, var2=value2, ...} ", ex);
            }
        }
    }

    public static void main(String[] args) {
        StringTokenizer tokenizer = new StringTokenizer("a,bc;ddw.cce,qwwer1/wr", ",;.");
        while (tokenizer.hasMoreTokens()) {
            System.out.println(tokenizer.nextToken());
        }
    }

}
