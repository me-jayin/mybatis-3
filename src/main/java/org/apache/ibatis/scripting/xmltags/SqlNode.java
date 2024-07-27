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

/**
 * @author Clinton Begin
 */
public interface SqlNode {
  /**
   * 应用 sql 节点，通常如果条件满足，则会调用 {@link DynamicContext#appendSql(String)} 方法拼接sql
   * 该方法执行后只是得到最终包含占位符的sql语句，如
   *     select * from a where c = #{c}
   * 后面还需要基于该sql、入参解析其占位符，得到 ParameterMapping 参数映射列表
   * @param context
   * @return
   */
  boolean apply(DynamicContext context);
}
