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
package org.apache.ibatis.scripting.xmltags;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * xml的sql脚本构建器，通过解析xml语句标签块中的 if、trim 等标签，来封装成一个 MixedSqlNode
 *
 * @author Clinton Begin
 */
public class XMLScriptBuilder extends BaseBuilder {
    /**
     * sql节点上下文
     */
    private final XNode context;
    /**
     * 是否是动态sql，会根据该自动创建对应的 SqlSource 对象
     */
    private boolean isDynamic;
    /**
     * 参数类型
     */
    private final Class<?> parameterType;
    /**
     * 节点处理器 map
     */
    private final Map<String, NodeHandler> nodeHandlerMap = new HashMap<>();

    public XMLScriptBuilder(Configuration configuration, XNode context) {
        this(configuration, context, null);
    }

    public XMLScriptBuilder(Configuration configuration, XNode context, Class<?> parameterType) {
        super(configuration);
        this.context = context;
        this.parameterType = parameterType;
        initNodeHandlerMap();
    }

    /**
     * 初始化节点 NodeHandler 映射表，NodeHandler 能够按类型来解析对应的 动态标签，从而生成对应的 SqlNode
     */
    private void initNodeHandlerMap() {
        nodeHandlerMap.put("trim", new TrimHandler());
        nodeHandlerMap.put("where", new WhereHandler());
        nodeHandlerMap.put("set", new SetHandler());
        nodeHandlerMap.put("foreach", new ForEachHandler());
        nodeHandlerMap.put("if", new IfHandler());
        nodeHandlerMap.put("choose", new ChooseHandler());
        // 注意，when实际上就是 if
        nodeHandlerMap.put("when", new IfHandler());
        // choose 里的 otherwise
        nodeHandlerMap.put("otherwise", new OtherwiseHandler());
        // 绑定属性标签
        nodeHandlerMap.put("bind", new BindHandler());
    }

    /**
     * 解析xml语句标签块，从而得到最终的 SqlSource
     * @return
     */
    public SqlSource parseScriptNode() {
        // 开始解析动态标签
        MixedSqlNode rootSqlNode = parseDynamicTags(context);
        SqlSource sqlSource;
        if (isDynamic) {
            sqlSource = new DynamicSqlSource(configuration, rootSqlNode);
        } else {
            sqlSource = new RawSqlSource(configuration, rootSqlNode, parameterType);
        }
        return sqlSource;
    }

    /**
     * 解析xml标签内的动态标签
     * @param node
     * @return
     */
    protected MixedSqlNode parseDynamicTags(XNode node) {
        List<SqlNode> contents = new ArrayList<>();
        NodeList children = node.getNode().getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            XNode child = node.newXNode(children.item(i));
            // 如果是 CDATA 、TEXT 标签则直接创建一个 TextSqlNode，并且判断该文本中，是否包含动态参数（${}）；如果是则直接添加一个 TextSqlNode，否则添加 StaticTextSqlNode3
            // 因为 TextSqlNode#apply(DynamicContext context) 中会对 ${} 占位符进行处理，
            // 因此如果不是动态语句，则直接使用 StaticTextSqlNode（apply方法中不做任何处理，直接返回 sql），能够节省对 ${} 占位符的处理
            if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE || child.getNode().getNodeType() == Node.TEXT_NODE) {
                String data = child.getStringBody("");
                TextSqlNode textSqlNode = new TextSqlNode(data);
                if (textSqlNode.isDynamic()) {
                    contents.add(textSqlNode);
                    isDynamic = true;
                } else {
                    // 添加静态的 TextSqlNode
                    contents.add(new StaticTextSqlNode(data));
                }
            } else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) { // 如果是其他元素标签，则使用对应的 NodeHandler 来处理
                // 根据标签名称，获取对应的 NodeHandler
                String nodeName = child.getNode().getNodeName();
                NodeHandler handler = nodeHandlerMap.get(nodeName);
                if (handler == null) {
                    throw new BuilderException("Unknown element <" + nodeName + "> in SQL statement.");
                }
                // 使用NodeHandler对当前节点进行解析，并标记为动态SqlNode
                handler.handleNode(child, contents);
                isDynamic = true;
            }
        }
        return new MixedSqlNode(contents);
    }

    private interface NodeHandler {
        void handleNode(XNode nodeToHandle, List<SqlNode> targetContents);
    }

    /**
     * bind标签，可往参数中追加额外变量
     * <bind name="newParam" value="oldParam + '1'" />
     */
    private static class BindHandler implements NodeHandler {
        public BindHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            final String name = nodeToHandle.getStringAttribute("name");
            final String expression = nodeToHandle.getStringAttribute("value");
            final VarDeclSqlNode node = new VarDeclSqlNode(name, expression);
            targetContents.add(node);
        }
    }

    /**
     * trim标签，可以去除字符串前后特殊字符，并且trim标签可以内嵌其他标签
     */
    private class TrimHandler implements NodeHandler {
        public TrimHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            // 先解析内嵌的标签
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            // 获取trim标签去除的前后缀，并获取需要追加的前后缀字符串
            String prefix = nodeToHandle.getStringAttribute("prefix");
            String prefixOverrides = nodeToHandle.getStringAttribute("prefixOverrides"); // 需要去除的前缀
            String suffix = nodeToHandle.getStringAttribute("suffix");
            String suffixOverrides = nodeToHandle.getStringAttribute("suffixOverrides"); // 需要去除的后缀
            // 基于内嵌的标签、前后缀及追加值，创建一个 TrimSqlNode
            TrimSqlNode trim = new TrimSqlNode(configuration, mixedSqlNode, prefix, prefixOverrides, suffix, suffixOverrides);
            targetContents.add(trim);
        }
    }

    /**
     * where 标签，会增加 where，并剔除 and、or
     * <select id="selectUsers" resultType="User">
     *     SELECT * FROM users
     *     <where>
     *         <if test="name != null">
     *             AND name = #{name}
     *         </if>
     *         <if test="email != null">
     *             AND email = #{email}
     *         </if>
     *     </where>
     * </select>
     */
    private class WhereHandler implements NodeHandler {
        public WhereHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            // 解析内嵌标签
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            // 构建一个包装后的 trim 子类
            WhereSqlNode where = new WhereSqlNode(configuration, mixedSqlNode);
            targetContents.add(where);
        }
    }

    /**
     * set标签，在 update 中使用，功能就是剔除后面的 ,
     * <update id="updateUser" parameterType="User">
     *     update user
     *     <set>
     *         <if test="username != null">username=#{username},</if>
     *         <if test="password != null">password=#{password},</if>
     *         <if test="email != null">email=#{email},</if>
     *     </set>
     *     where id=#{id}
     * </update>
     */
    private class SetHandler implements NodeHandler {
        public SetHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            SetSqlNode set = new SetSqlNode(configuration, mixedSqlNode);
            targetContents.add(set);
        }
    }

    /**
     * foreach 标签
     * <foreach collection="array" open="(" separator="," close=")" item="item" index="index">
     *     #{item,jdbcType=VARCHAR}
     * </foreach>
     */
    private class ForEachHandler implements NodeHandler {
        public ForEachHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            // 解析内嵌标签
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            // 获取collection目标属性、对应的属性是否允许为null
            String collection = nodeToHandle.getStringAttribute("collection");
            Boolean nullable = nodeToHandle.getBooleanAttribute("nullable");
            // 指定遍历collection时存放每一项的参数名称、当前下标的参数名
            String item = nodeToHandle.getStringAttribute("item");
            String index = nodeToHandle.getStringAttribute("index");
            // 遍历时起始、末尾字符
            String open = nodeToHandle.getStringAttribute("open");
            String close = nodeToHandle.getStringAttribute("close");
            // 遍历时，执行完每一项的内嵌标签后的分隔符
            String separator = nodeToHandle.getStringAttribute("separator");

            ForEachSqlNode forEachSqlNode = new ForEachSqlNode(configuration, mixedSqlNode, collection, nullable, index, item,
                    open, close, separator);
            targetContents.add(forEachSqlNode);
        }
    }

    /**
     * if 判断 handler
     */
    private class IfHandler implements NodeHandler {
        public IfHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            // 解析内嵌标签
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            // 获取 test属性
            String test = nodeToHandle.getStringAttribute("test");
            // 创建 IfSqlNode 并添加
            IfSqlNode ifSqlNode = new IfSqlNode(mixedSqlNode, test);
            targetContents.add(ifSqlNode);
        }
    }

    /**
     * otherwise标签
     */
    private class OtherwiseHandler implements NodeHandler {
        public OtherwiseHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            // 直接解析嵌套标签
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            targetContents.add(mixedSqlNode);
        }
    }

    /**
     * choose 标签，注意，其中 when 实际上就和 if 标签一样
     */
    private class ChooseHandler implements NodeHandler {
        public ChooseHandler() {
            // Prevent Synthetic Access
        }

        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            List<SqlNode> whenSqlNodes = new ArrayList<>();
            List<SqlNode> otherwiseSqlNodes = new ArrayList<>();
            // 遍历子标签，分别处理 when 、 otherwise 标签
            handleWhenOtherwiseNodes(nodeToHandle, whenSqlNodes, otherwiseSqlNodes);
            // 获取 otherwise 标签的SqlNode
            SqlNode defaultSqlNode = getDefaultSqlNode(otherwiseSqlNodes);
            ChooseSqlNode chooseSqlNode = new ChooseSqlNode(whenSqlNodes, defaultSqlNode);
            targetContents.add(chooseSqlNode);
        }

        private void handleWhenOtherwiseNodes(XNode chooseSqlNode, List<SqlNode> ifSqlNodes,
                                              List<SqlNode> defaultSqlNodes) {
            List<XNode> children = chooseSqlNode.getChildren();
            for (XNode child : children) {
                String nodeName = child.getNode().getNodeName();
                NodeHandler handler = nodeHandlerMap.get(nodeName);
                if (handler instanceof IfHandler) {
                    handler.handleNode(child, ifSqlNodes);
                } else if (handler instanceof OtherwiseHandler) {
                    handler.handleNode(child, defaultSqlNodes);
                }
            }
        }

        private SqlNode getDefaultSqlNode(List<SqlNode> defaultSqlNodes) {
            SqlNode defaultSqlNode = null;
            if (defaultSqlNodes.size() == 1) {
                defaultSqlNode = defaultSqlNodes.get(0);
            } else if (defaultSqlNodes.size() > 1) {
                throw new BuilderException("Too many default (otherwise) elements in choose statement.");
            }
            return defaultSqlNode;
        }
    }

}
