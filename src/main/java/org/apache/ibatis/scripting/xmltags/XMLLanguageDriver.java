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

import org.apache.ibatis.builder.xml.XMLMapperEntityResolver;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;

/**
 * 静态SQL：预编译的SQL语句，不会根据输入参数发生变化。如：
 * 1.select * from a
 * 2.select * from a where c = #{usr}
 * 3.select * from a where c = ?
 * 上面的sql语句基本固定，可以转为同 3 相同的预编译的sql语句
 *
 * 动态SQL：根据输入参数生成的SQL语句，可以根据不同的参数值生成不同的SQL语句。如：
 * 1.select * from ${table}
 * 这种sql语句会根据入参的变化而变化，如 table = a 时，sql为 select * from a，如果 table = b，则 select * from b
 * 2.select * from a <where> <if test="usr != null">c != #{usr}</if> </where>
 * 这种包含sql标签的sql，也是动态sql
 * <p>
 * 而{@link XMLLanguageDriver}支持对动态SQL的处理，而{@link org.apache.ibatis.scripting.defaults.RawLanguageDriver} 只支持对静态SQL的处理
 *
 * @author Eduardo Macarron
 */
public class XMLLanguageDriver implements LanguageDriver {

    @Override
    public ParameterHandler createParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
        return new DefaultParameterHandler(mappedStatement, parameterObject, boundSql);
    }

    /**
     * 基于XML节点构建一个 SqlSource 对象，该方法会判断 xml 节点中是否包含 ${} 或者 <if> 等动态标签，来判断sql是否是动态sql
     *
     * @param configuration The MyBatis configuration
     * @param script        XNode parsed from a XML file
     * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be
     *                      null.
     * @return
     */
    @Override
    public SqlSource createSqlSource(Configuration configuration, XNode script, Class<?> parameterType) {
        // 按xml脚本解析SqlSource
        XMLScriptBuilder builder = new XMLScriptBuilder(configuration, script, parameterType);
        return builder.parseScriptNode();
    }

    /**
     * 用于兼容使用 {@link org.apache.ibatis.annotations.Mapper} 注解来声明 sql 的方式
     *
     * @param configuration
     * @param script        脚本语句
     * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be
     *                      null.
     * @return
     */
    @Override
    public SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType) {
        // 如果是 <script> 包裹的 string，则当成一个 xpath 节点处理
        if (script.startsWith("<script>")) {
            XPathParser parser = new XPathParser(script, false, configuration.getVariables(), new XMLMapperEntityResolver());
            return createSqlSource(configuration, parser.evalNode("/script"), parameterType);
        }
        // 否则对数据进行占位符替换，并直接创建一个 TextSqlNode
        script = PropertyParser.parse(script, configuration.getVariables());
        TextSqlNode textSqlNode = new TextSqlNode(script);
        // 判断是否包含 ${}，如果是则说明是动态sql
        if (textSqlNode.isDynamic()) {
            return new DynamicSqlSource(configuration, textSqlNode);
        } else {
            return new RawSqlSource(configuration, script, parameterType);
        }
    }

}
