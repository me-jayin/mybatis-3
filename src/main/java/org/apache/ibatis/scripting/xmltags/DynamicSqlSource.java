/*
 *    Copyright 2009-2022 the original author or authors.
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

import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

/**
 * 动态的SqlSource，其中能力由 SqlNode 来控制
 * @author Clinton Begin
 */
public class DynamicSqlSource implements SqlSource {

    private final Configuration configuration;
    /**
     * SqlNode节点，记录sql标签的节点描述。
     * 如果语句中不含动态参数、标签，则该 SqlNode 类型为：TextSqlNode
     * 如果不包含则是 StaticTextSqlNode 类型
     * 如果是基于xml标签解析的则是 MixedSqlNode，并且 MixedSqlNode 中持有一个 SqlNode 集合，用于描述xml标签中的所有节点描述，如if、trim、where等
     */
    private final SqlNode rootSqlNode;

    public DynamicSqlSource(Configuration configuration, SqlNode rootSqlNode) {
        this.configuration = configuration;
        this.rootSqlNode = rootSqlNode;
    }

    /**
     * 基于方法入参，构建一个 SqlSource。
     * 该方法实际上会先基于应用所有SqlNode，然后再将最终只包含#{}的sql语句，通过 SqlSourceBuilder 替换并解析sql，将#{}占位符替换 ? 并得到参数映射列表后，构建一个 StaticSqlSource。
     * 再通过 StaticSqlSource 的 getBoundSql 直接获取一个 BoundSql 对象
     * @param parameterObject
     * @return
     */
    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        // 构建 DynamicContext，并应用 SqlNode，得到最终只含 #{} 占位符的sql语句
        DynamicContext context = new DynamicContext(configuration, parameterObject);
        rootSqlNode.apply(context);
        // 最终只含 #{} 占位符的sql语句，实际上就是一个静态sql，因此直接通过 SqlSourceBuilder ，构建一个 StaticSqlSource
        SqlSourceBuilder sqlSourceParser = new SqlSourceBuilder(configuration);
        Class<?> parameterType = parameterObject == null ? Object.class : parameterObject.getClass();
        // 注意，SqlSourceBuilder.parse 构建 StaticSqlSource 时会解析占位符并替换占位符，得到一个只含 ? 的预处理sql，以及记录每个占位符与参数映射关系的集合
        SqlSource sqlSource = sqlSourceParser.parse(context.getSql(), parameterType, context.getBindings());
        // 直接通过StaticSqlSource的 getBoundSql 构建 BoundSql 对象
        BoundSql boundSql = sqlSource.getBoundSql(parameterObject);
        // 由于创建 BoundSql 时传入的参数仅是 入参，因此需要将原 DynamicContext 中 bindings 里的所有key-value参数对都作为附加参数，添加到 BoundSql 中
        context.getBindings().forEach(boundSql::setAdditionalParameter);
        return boundSql;
    }

}
