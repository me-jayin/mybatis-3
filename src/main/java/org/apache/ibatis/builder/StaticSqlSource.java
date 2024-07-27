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
package org.apache.ibatis.builder;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

import java.util.List;

/**
 * 静态SQL，因为不存在${}占位符，以及动态SQL标签，所以SQL语句固定。
 * 因此直接将所有 #{} 解析出对应的 ParameterMapping 即可，从而提高后续解析效率
 * @author Clinton Begin
 */
public class StaticSqlSource implements SqlSource {
  /**
   * 替换了 #{} 占位符的表达式，如： select * from a where c = ?
   * 只包含 ? 预处理器占位符
   */
  private final String sql;
  /**
   * 记录预处理占位符与参数映射关系
   */
  private final List<ParameterMapping> parameterMappings;
  private final Configuration configuration;

  public StaticSqlSource(Configuration configuration, String sql) {
    this(configuration, sql, null);
  }

  public StaticSqlSource(Configuration configuration, String sql, List<ParameterMapping> parameterMappings) {
    this.sql = sql;
    this.parameterMappings = parameterMappings;
    this.configuration = configuration;
  }

  /**
   *
   * @param parameterObject
   * @return
   */
  @Override
  public BoundSql getBoundSql(Object parameterObject) {
    // 基于已解析的 ParameterMapping 生成 BoundSql 的
    return new BoundSql(configuration, sql, parameterMappings, parameterObject);
  }

}
