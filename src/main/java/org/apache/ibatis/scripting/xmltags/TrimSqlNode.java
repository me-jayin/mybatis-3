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

import org.apache.ibatis.session.Configuration;

import java.util.*;

/**
 * trim 标签 SqlNode
 *
 * @author Clinton Begin
 */
public class TrimSqlNode implements SqlNode {
    /**
     * 内嵌的标签解析得到的 SqlNode
     */
    private final SqlNode contents;
    /**
     * 追加的前缀
     */
    private final String prefix;
    /**
     * 追加的后缀
     */
    private final String suffix;
    /**
     * 需要去除的前缀
     */
    private final List<String> prefixesToOverride;
    /**
     * 去除的后缀
     */
    private final List<String> suffixesToOverride;
    private final Configuration configuration;

    public TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, String prefixesToOverride,
                       String suffix, String suffixesToOverride) {
        this(configuration, contents, prefix, parseOverrides(prefixesToOverride), suffix,
                parseOverrides(suffixesToOverride));
    }

    protected TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, List<String> prefixesToOverride,
                          String suffix, List<String> suffixesToOverride) {
        this.contents = contents;
        this.prefix = prefix;
        this.prefixesToOverride = prefixesToOverride;
        this.suffix = suffix;
        this.suffixesToOverride = suffixesToOverride;
        this.configuration = configuration;
    }

    @Override
    public boolean apply(DynamicContext context) {
        // 对原上下文进行包装，然后先执行内嵌的标签
        // 包装的目的是将 内嵌标签执行后需要追加的sql 先缓存起来，最后应用当前 TrimSqlNode 时，对缓存的 sql 执行 trim 操作
        // 最后再将 trim 操作后的缓存sql，应用到被包装的 context 中即可
        FilteredDynamicContext filteredDynamicContext = new FilteredDynamicContext(context);
        boolean result = contents.apply(filteredDynamicContext);
        filteredDynamicContext.applyAll();
        return result;
    }

    /**
     * 多个可用 | 进行分隔
     * @param overrides
     * @return
     */
    private static List<String> parseOverrides(String overrides) {
        if (overrides != null) {
            final StringTokenizer parser = new StringTokenizer(overrides, "|", false);
            final List<String> list = new ArrayList<>(parser.countTokens());
            while (parser.hasMoreTokens()) {
                list.add(parser.nextToken().toUpperCase(Locale.ENGLISH));
            }
            return list;
        }
        return Collections.emptyList();
    }

    private class FilteredDynamicContext extends DynamicContext {
        private final DynamicContext delegate;
        private boolean prefixApplied;
        private boolean suffixApplied;
        /**
         * 保存trim中子标签应用时得到 sql
         */
        private StringBuilder sqlBuffer;

        public FilteredDynamicContext(DynamicContext delegate) {
            super(configuration, null);
            this.delegate = delegate;
            this.prefixApplied = false;
            this.suffixApplied = false;
            this.sqlBuffer = new StringBuilder();
        }

        /**
         * 应用所有，对缓存buffer进行trim操作
         */
        public void applyAll() {
            sqlBuffer = new StringBuilder(sqlBuffer.toString().trim());
            String trimmedUppercaseSql = sqlBuffer.toString().toUpperCase(Locale.ENGLISH);
            if (trimmedUppercaseSql.length() > 0) {
                applyPrefix(sqlBuffer, trimmedUppercaseSql);
                applySuffix(sqlBuffer, trimmedUppercaseSql);
            }
            delegate.appendSql(sqlBuffer.toString());
        }

        @Override
        public Map<String, Object> getBindings() {
            return delegate.getBindings();
        }

        @Override
        public void bind(String name, Object value) {
            delegate.bind(name, value);
        }

        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }

        /**
         * 重点在这，将处理的sql追加到缓存buffer中
         * @param sql
         */
        @Override
        public void appendSql(String sql) {
            sqlBuffer.append(sql);
        }

        @Override
        public String getSql() {
            return delegate.getSql();
        }

        private void applyPrefix(StringBuilder sql, String trimmedUppercaseSql) {
            if (prefixApplied) { // 处理过前缀则后面无需处理
                return;
            }
            prefixApplied = true;
            // 找到第一个移除的前缀进行移除
            if (prefixesToOverride != null) {
                prefixesToOverride.stream().filter(trimmedUppercaseSql::startsWith).findFirst()
                        .ifPresent(toRemove -> sql.delete(0, toRemove.trim().length()));
            }
            // 往前面追加指定前缀
            if (prefix != null) {
                sql.insert(0, " ").insert(0, prefix);
            }
        }

        // 同 applyPrefix
        private void applySuffix(StringBuilder sql, String trimmedUppercaseSql) {
            if (suffixApplied) {
                return;
            }
            suffixApplied = true;
            if (suffixesToOverride != null) {
                suffixesToOverride.stream()
                        .filter(toRemove -> trimmedUppercaseSql.endsWith(toRemove) || trimmedUppercaseSql.endsWith(toRemove.trim()))
                        .findFirst().ifPresent(toRemove -> {
                            int start = sql.length() - toRemove.trim().length();
                            int end = sql.length();
                            sql.delete(start, end);
                        });
            }
            if (suffix != null) {
                sql.append(" ").append(suffix);
            }
        }

    }

}
