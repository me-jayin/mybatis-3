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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Frank D. Martinez [mnesarco]
 */
public class XMLIncludeTransformer {

    private final Configuration configuration;
    private final MapperBuilderAssistant builderAssistant;

    public XMLIncludeTransformer(Configuration configuration, MapperBuilderAssistant builderAssistant) {
        this.configuration = configuration;
        this.builderAssistant = builderAssistant;
    }

    /**
     * 应用（替换） include 标签
     *
     * @param source
     */
    public void applyIncludes(Node source) {
        // 获取全局的变量
        Properties variablesContext = new Properties();
        Properties configurationVariables = configuration.getVariables();
        Optional.ofNullable(configurationVariables).ifPresent(variablesContext::putAll);
        // 应用include
        applyIncludes(source, variablesContext, false);
    }

    /**
     * Recursively apply includes through all SQL fragments.
     * 递归应用 include 标签，如
     * <include refid="selectSql">
     *     <property name="usr" value="admin" />
     * </include>
     * <sql id="selectSql">
     *     select * from user
     *     <include refid="condition" />
     * </sql>
     * <sql id="condition>
     *     where usr = #{usr}
     * </sql>
     *
     * 该方法会递归将 include 方法进行替换并处理占位符，最终结果：
     * select * from user where usr = admin
     *
     * @param source           Include node in DOM tree
     * @param variablesContext Current context for static variables with values
     * @param included 当前的节点是否是include的子节点，如 <include> <if test=""><include/></if> </include>
     */
    private void applyIncludes(Node source, final Properties variablesContext, boolean included) {
        if ("include".equals(source.getNodeName())) {
            // 查找 include refid 对应的 sql 标签
            Node toInclude = findSqlFragment(getStringAttribute(source, "refid"), variablesContext);
            // 解析 property 的值并加入 变量上下文
            Properties toIncludeContext = getVariablesContext(source, variablesContext);
            // 对 include 标签引用的 sql 递归进行 include 替换
            applyIncludes(toInclude, toIncludeContext, true);
            // 如果 被引用的sql节点 和 include节点 不在同一个文档，则需要将 被引用的sql节点 引入 include节点 的文档中
            if (toInclude.getOwnerDocument() != source.getOwnerDocument()) {
                toInclude = source.getOwnerDocument().importNode(toInclude, true);
            }
            // 获取source节点的父节点，并将source节点替换为进行include替换后的 sql 节点
            source.getParentNode().replaceChild(toInclude, source);
            while (toInclude.hasChildNodes()) {
                toInclude.getParentNode().insertBefore(toInclude.getFirstChild(), toInclude);
            }
            toInclude.getParentNode().removeChild(toInclude);
        } else if (source.getNodeType() == Node.ELEMENT_NODE) { // 如果非 include 标签，并且类型是 元素标签类型，则遍历其子节点
            // 如果当前操作的节点是<include />引入的<sql />节点（包括sql标签再引用的节点），并且全局变量不为空，则进行属性替换操作，
            // 例如 <sql id="123" lang="${sys.lang}"> 的情况，lang 属性是可以被替换的
            if (included && !variablesContext.isEmpty()) {
                // 对属性值进行解析，因此被引用的标签属性可以用占位符 ${xxx} 来设置，如 <sql databaseId="${dbId}" />
                NamedNodeMap attributes = source.getAttributes();
                for (int i = 0; i < attributes.getLength(); i++) {
                    Node attr = attributes.item(i);
                    attr.setNodeValue(PropertyParser.parse(attr.getNodeValue(), variablesContext));
                }
            }
            NodeList children = source.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                applyIncludes(children.item(i), variablesContext, included);
            }
        } else if (included && (source.getNodeType() == Node.TEXT_NODE || source.getNodeType() == Node.CDATA_SECTION_NODE)
                && !variablesContext.isEmpty()) {
            // 如果是 text 和 cdata 节点，则直接进行占位符替换
            // 注意，这也导致如果sql的参数用的（例如 where user_id = ${userId}），并且全局配置中也有对应的属性（userId: 123）时，将会出现
            // sql语句的占位符会在这步优先被替换为全局配置的属性值（where user_d = 123 ）
            source.setNodeValue(PropertyParser.parse(source.getNodeValue(), variablesContext));
        }
    }

    private Node findSqlFragment(String refid, Properties variables) {
        refid = PropertyParser.parse(refid, variables);
        refid = builderAssistant.applyCurrentNamespace(refid, true);
        try {
            XNode nodeToInclude = configuration.getSqlFragments().get(refid);
            return nodeToInclude.getNode().cloneNode(true);
        } catch (IllegalArgumentException e) {
            throw new IncompleteElementException("Could not find SQL statement to include with refid '" + refid + "'", e);
        }
    }

    private String getStringAttribute(Node node, String name) {
        return node.getAttributes().getNamedItem(name).getNodeValue();
    }

    /**
     * Read placeholders and their values from include node definition.
     * 获取子标签 property 的name、value，对value进行占位符替换，最后将其添加到变量上下文中
     * @param node                      Include node instance
     * @param inheritedVariablesContext Current context used for replace variables in new variables values
     * @return variables context from include instance (no inherited values)
     */
    private Properties getVariablesContext(Node node, Properties inheritedVariablesContext) {
        Map<String, String> declaredProperties = null;
        NodeList children = node.getChildNodes();
        // 获取 include 子标签
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            // 如果是标签，即<property />
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                // 获取 name
                String name = getStringAttribute(n, "name");
                // 获取 value 值，并使用 Configuration 进行占位符 ${xxx} 替换操作
                String value = PropertyParser.parse(getStringAttribute(n, "value"), inheritedVariablesContext);
                if (declaredProperties == null) {
                    declaredProperties = new HashMap<>();
                }
                // 添加property的name、value，如果重复定义则抛出异常
                if (declaredProperties.put(name, value) != null) {
                    throw new BuilderException("Variable " + name + " defined twice in the same include definition");
                }
            }
        }
        if (declaredProperties == null) {
            return inheritedVariablesContext;
        }
        Properties newProperties = new Properties();
        newProperties.putAll(inheritedVariablesContext);
        newProperties.putAll(declaredProperties);
        return newProperties;
    }
}
