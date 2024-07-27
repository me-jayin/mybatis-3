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

import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.session.Configuration;

import java.util.Map;
import java.util.Optional;

/**
 * foreach标签
 * 在标签解析后，会将嵌套表达式中的 item、index 占位符替换为最终实际值的 变量名
 *
 * @author Clinton Begin
 */
public class ForEachSqlNode implements SqlNode {
    /**
     * 项的前缀，防止与普通变量发生重复
     */
    public static final String ITEM_PREFIX = "__frch_";

    /**
     *
     */
    private final ExpressionEvaluator evaluator;
    /**
     * 集合所在变量表达式
     */
    private final String collectionExpression;
    /**
     * 是否允许集合为空
     */
    private final Boolean nullable;
    /**
     * 嵌套的xml标签解析后的结果
     */
    private final SqlNode contents;
    private final String open;
    private final String close;
    private final String separator;
    private final String item;
    private final String index;
    private final Configuration configuration;

    /**
     * @deprecated Since 3.5.9, use the
     * {@link #ForEachSqlNode(Configuration, SqlNode, String, Boolean, String, String, String, String, String)}.
     */
    @Deprecated
    public ForEachSqlNode(Configuration configuration, SqlNode contents, String collectionExpression, String index,
                          String item, String open, String close, String separator) {
        this(configuration, contents, collectionExpression, null, index, item, open, close, separator);
    }

    /**
     * @since 3.5.9
     */
    public ForEachSqlNode(Configuration configuration, SqlNode contents, String collectionExpression, Boolean nullable,
                          String index, String item, String open, String close, String separator) {
        this.evaluator = new ExpressionEvaluator();
        this.collectionExpression = collectionExpression;
        this.nullable = nullable;
        this.contents = contents;
        this.open = open;
        this.close = close;
        this.separator = separator;
        this.index = index;
        this.item = item;
        this.configuration = configuration;
    }

    @Override
    public boolean apply(DynamicContext context) {
        // 获取属性绑定集
        Map<String, Object> bindings = context.getBindings();
        // 获取对应集合对象
        final Iterable<?> iterable = evaluator.evaluateIterable(collectionExpression, bindings,
                Optional.ofNullable(nullable).orElseGet(configuration::isNullableOnForEach));
        // 无数据或为空直接退出
        if (iterable == null || !iterable.iterator().hasNext()) {
            return true;
        }
        // 否则进行迭代
        boolean first = true;
        // 添加 open 值
        applyOpen(context);
        int i = 0;
        for (Object o : iterable) {
            DynamicContext oldContext = context;
            // 根据迭代次数，判断是否在桥接 sql 前需要执行前置符，即是否需要添加分隔符
            if (first || separator == null) {
                context = new PrefixedContext(context, "");
            } else {
                context = new PrefixedContext(context, separator);
            }

            // 唯一值，由于可能会有多层嵌套 foreach，所以生成 __frch_{type}_{idx} 的变量时，将使用该唯一值来确保值的唯一性
            int uniqueNumber = context.getUniqueNumber();
            // 判断实现类型，来获取下标和值（如果是Map数据，则下标为key）
            if (o instanceof Map.Entry) {
                Map.Entry<Object, Object> mapEntry = (Map.Entry<Object, Object>) o;
                applyIndex(context, mapEntry.getKey(), uniqueNumber);
                applyItem(context, mapEntry.getValue(), uniqueNumber);
            } else {
                applyIndex(context, i, uniqueNumber);
                applyItem(context, o, uniqueNumber);
            }
            // 对 context 再包装，然后应用嵌套 SqlNode，在这里会对内嵌表达式中 item、index 的变量名称，转为 __frch_{type}_{uniqueNumber} 格式的变量名称
            contents.apply(new FilteredDynamicContext(configuration, context, index, item, uniqueNumber));
            if (first) {
                first = !((PrefixedContext) context).isPrefixApplied();
            }
            context = oldContext;
            i++;
        }
        // 添加 close 值
        applyClose(context);
        context.getBindings().remove(item);
        context.getBindings().remove(index);
        return true;
    }

    /**
     * 往 context 中添加下标，会添加两个key-value对：
     * 1.{index}-value
     * 2.__frch_{index}_{uniqueNumber}
     * @param context
     * @param o
     * @param i
     */
    private void applyIndex(DynamicContext context, Object o, int i) {
        if (index != null) {
            // 往顶层添加属性，不会影响 _parameter 的值
            context.bind(index, o);
            context.bind(itemizeItem(index, i), o);
        }
    }

    /**
     * 往 context 中添加 value
     * @param context
     * @param o
     * @param i
     */
    private void applyItem(DynamicContext context, Object o, int i) {
        if (item != null) {
            context.bind(item, o);
            context.bind(itemizeItem(item, i), o);
        }
    }

    private void applyOpen(DynamicContext context) {
        if (open != null) {
            context.appendSql(open);
        }
    }

    private void applyClose(DynamicContext context) {
        if (close != null) {
            context.appendSql(close);
        }
    }

    /**
     *
     * @param item
     * @param i
     * @return
     */
    private static String itemizeItem(String item, int i) {
        return ITEM_PREFIX + item + "_" + i;
    }

    /**
     * DynamicContext包装类，会再
     */
    private static class FilteredDynamicContext extends DynamicContext {
        private final DynamicContext delegate;
        private final int index;
        private final String itemIndex;
        private final String item;

        public FilteredDynamicContext(Configuration configuration, DynamicContext delegate, String itemIndex, String item,
                                      int i) {
            super(configuration, null);
            this.delegate = delegate;
            this.index = i;
            this.itemIndex = itemIndex;
            this.item = item;
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
        public String getSql() {
            return delegate.getSql();
        }

        @Override
        public void appendSql(String sql) {
            // 声明 #{} 表达式解析器
            GenericTokenParser parser = new GenericTokenParser("#{", "}", content -> {
                // 分别替换 item、index 对应的占位符的变量名，由简单的 item、index 名称，修改为 __frch_{name}_{uniqueNum} 格式的名称
                String newContent = content.replaceFirst("^\\s*" + item + "(?![^.,:\\s])", itemizeItem(item, index));
                if (itemIndex != null && newContent.equals(content)) {
                    newContent = content.replaceFirst("^\\s*" + itemIndex + "(?![^.,:\\s])", itemizeItem(itemIndex, index));
                }
                return "#{" + newContent + "}";
            });

            delegate.appendSql(parser.parse(sql));
        }

        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }

    }

    /**
     * 对 DynamicContext 进行包装，在 appendSql 前会判断是否需要添加 前置符，即分隔符
     */
    private class PrefixedContext extends DynamicContext {
        private final DynamicContext delegate;
        private final String prefix;
        private boolean prefixApplied;

        public PrefixedContext(DynamicContext delegate, String prefix) {
            super(configuration, null);
            this.delegate = delegate;
            this.prefix = prefix;
            this.prefixApplied = false;
        }

        public boolean isPrefixApplied() {
            return prefixApplied;
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
        public void appendSql(String sql) {
            // 添加时会判断是否是首次添加，如果是则会先添加 separator
            if (!prefixApplied && sql != null && sql.trim().length() > 0) {
                delegate.appendSql(prefix);
                prefixApplied = true;
            }
            delegate.appendSql(sql);
        }

        @Override
        public String getSql() {
            return delegate.getSql();
        }

        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }
    }

}
