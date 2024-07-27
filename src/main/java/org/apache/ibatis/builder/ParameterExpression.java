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

import java.util.HashMap;

/**
 * Inline parameter expression parser. Supported grammar (simplified):
 *
 * <pre>
 * inline-parameter = (propertyName | expression) oldJdbcType attributes
 * propertyName = /expression language's property navigation path/
 * expression = '(' /expression language's expression/ ')'
 * oldJdbcType = ':' /any valid jdbc type/
 * attributes = (',' attribute)*
 * attribute = name '=' value
 * </pre>
 *
 * 参数表表达式解析，格式： (属性名称或表达式):jdbcType, option=value
 *
 * 例如：(id.toString()):VARCHAR, attr1=val1, attr2=val2
 * 解析后结果是：
 * expression: id.toString()
 * jdbcType: VARCHAR
 * attr2:val2
 * attr1:val1
 *
 * 例如2：
 * id:VARCHAR, attr1=val1, attr2=val2
 * property: id
 * jdbcType: VARCHAR
 * attr1:val1
 * attr2:val2
 *
 * 例如3：
 * id, jdbcType=VARCHAR, attr1=val1, attr2=val2
 * property: id
 * jdbcType: VARCHAR
 * attr1:val1
 * attr2:val2
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class ParameterExpression extends HashMap<String, String> {

  private static final long serialVersionUID = -2417552199605158680L;

  public ParameterExpression(String expression) {
    parse(expression);
  }

  private void parse(String expression) {
    // 跳过' '空格，返回非空字符串的位置
    int p = skipWS(expression, 0);
    // 如果存在 () ，说明表达式需要调用方法
    if (expression.charAt(p) == '(') {
      expression(expression, p + 1);
    } else {
      property(expression, p);
    }
  }

  /**
   * 解析表达式，存在 () 的表达式
   * @param expression
   * @param left
   */
  private void expression(String expression, int left) {
    int match = 1;
    int right = left + 1;
    while (match > 0) {
      if (expression.charAt(right) == ')') {
        match--;
      } else if (expression.charAt(right) == '(') {
        match++;
      }
      right++;
    }
    // 添加 expression 属性
    put("expression", expression.substring(left, right - 1));
    jdbcTypeOpt(expression, right);
  }

  /**
   * 解析属性值
   * @param expression
   * @param left
   */
  private void property(String expression, int left) {
    if (left < expression.length()) {
      // 找到分隔符的位置
      int right = skipUntil(expression, left, ",:");
      // 这个时候，已经找到了关于属性的表达式，如 user, jdbcType=VARCHAR
      // property为 user
      put("property", trimmedStr(expression, left, right));
      // 处理 jdbcType 选项
      jdbcTypeOpt(expression, right);
    }
  }

  /**
   * 往后找到第一个非空格的字符位置
   * @param expression
   * @param p
   * @return
   */
  private int skipWS(String expression, int p) {
    for (int i = p; i < expression.length(); i++) {
      if (expression.charAt(i) > 0x20) {
        return i;
      }
    }
    return expression.length();
  }

  /**
   * 往后找到指定字符的位置
   * @param expression
   * @param p
   * @param endChars
   * @return
   */
  private int skipUntil(String expression, int p, final String endChars) {
    for (int i = p; i < expression.length(); i++) {
      char c = expression.charAt(i);
      if (endChars.indexOf(c) > -1) {
        return i;
      }
    }
    return expression.length();
  }

  /**
   * 处理jdbcType可选值
   * @param expression
   * @param p
   */
  private void jdbcTypeOpt(String expression, int p) {
    p = skipWS(expression, p);
    if (p < expression.length()) {
      // 找到非空字符时，判断是否是 : ，如果是则表示是简单的 jdbcType 配置。如：user:VARCHAR，效果同 user, jdbcType=VARCHAR
      if (expression.charAt(p) == ':') {
        jdbcType(expression, p + 1);
      } else if (expression.charAt(p) == ',') {
        // 如果不含 : ，则采用 option 的方式读取 key value 值
        option(expression, p + 1);
      } else {
        throw new BuilderException("Parsing error in {" + expression + "} in position " + p);
      }
    }
  }

  private void jdbcType(String expression, int p) {
    int left = skipWS(expression, p);
    int right = skipUntil(expression, left, ",");
    if (right <= left) {
      throw new BuilderException("Parsing error in {" + expression + "} in position " + p);
    }
    // 直接获取 : 后面的类型
    put("jdbcType", trimmedStr(expression, left, right));
    // 处理 option 属性
    option(expression, right + 1);
  }

  private void option(String expression, int p) {
    int left = skipWS(expression, p);
    if (left < expression.length()) {
      // 解析 key=value 的值
      int right = skipUntil(expression, left, "=");
      String name = trimmedStr(expression, left, right);
      left = right + 1;
      right = skipUntil(expression, left, ",");
      String value = trimmedStr(expression, left, right);
      put(name, value);
      option(expression, right + 1);
    }
  }

  /**
   * 切割指定区间的字符，并去除前后空格
   * @param str
   * @param start
   * @param end
   * @return
   */
  private String trimmedStr(String str, int start, int end) {
    while (str.charAt(start) <= 0x20) {
      start++;
    }
    while (str.charAt(end - 1) <= 0x20) {
      end--;
    }
    return start >= end ? "" : str.substring(start, end);
  }

}
