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
package org.apache.ibatis.reflection;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

/**
 * 参数名称解析器
 */
public class ParamNameResolver {
    /**
     * 用于默认参数名字的前缀
     */
    public static final String GENERIC_NAME_PREFIX = "param";
    /**
     * 是否使用实际的参数名称
     */
    private final boolean useActualParamName;

    /**
     * 方法参数下标及参数名称的映射。
     * 其中参数名称为 @Param 指定的名称，如果不存在 @Param ，则使用计数器的值（计数时跳过特殊类型 RowBounds、ResultHandler）作为名称
     * <p>
     * The key is the index and the value is the name of the parameter.<br />
     * The name is obtained from {@link Param} if specified. When {@link Param} is not specified, the parameter index is
     * used. Note that this index could be different from the actual index when the method has special parameters (i.e.
     * {@link RowBounds} or {@link ResultHandler}).
     * </p>
     * <ul>
     * <li>aMethod(@Param("M") int a, @Param("N") int b) -&gt; {{0, "M"}, {1, "N"}}</li>
     * <li>aMethod(int a, int b) -&gt; {{0, "0"}, {1, "1"}}</li>
     * <li>aMethod(int a, RowBounds rb, int b) -&gt; {{0, "0"}, {2, "1"}}</li>
     * </ul>
     */
    private final SortedMap<Integer, String> names;
    /**
     * 是否存在 @Param 注解
     */
    private boolean hasParamAnnotation;

    public ParamNameResolver(Configuration config, Method method) {
        // 从 configuration 中获取 setting 的 useActualParamName 参数值，控制是否使用实际的参数名称
        this.useActualParamName = config.isUseActualParamName();
        // 获取参数类型及参数上的注解
        final Class<?>[] paramTypes = method.getParameterTypes();
        final Annotation[][] paramAnnotations = method.getParameterAnnotations();

        final SortedMap<Integer, String> map = new TreeMap<>();
        int paramCount = paramAnnotations.length;
        // 从 @Param 中获取参数名
        for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
            // 如果类型是 RowBounds、ResultHandler 的参数则跳过
            if (isSpecialParameter(paramTypes[paramIndex])) {
                continue;
            }
            String name = null;
            // 找到 @Param 注解，并获取值
            for (Annotation annotation : paramAnnotations[paramIndex]) {
                if (annotation instanceof Param) {
                    hasParamAnnotation = true;
                    name = ((Param) annotation).value();
                    break;
                }
            }
            // 对未使用 @Param 的方式处理，如果 useActualParamName 为true，则获取实际的参数名称
            // 如果获取不到实际参数名称，则使用参数下标作为参数名称
            if (name == null) {
                // @Param was not specified.
                if (useActualParamName) {
                    name = getActualParamName(method, paramIndex);
                }

                if (name == null) {
                    // use the parameter index as the name ("0", "1", ...)
                    // gcode issue #71
                    name = String.valueOf(map.size());
                }
            }
            // 保存下标和参数名的映射
            map.put(paramIndex, name);
        }
        // 置为无法修改的 map
        names = Collections.unmodifiableSortedMap(map);
    }

    /**
     * 获取实际的参数名
     * @param method
     * @param paramIndex
     * @return
     */
    private String getActualParamName(Method method, int paramIndex) {
        return ParamNameUtil.getParamNames(method).get(paramIndex);
    }

    /**
     * 判断类型是否是 RowBounds、ResultHandler 的参数
     * @param clazz
     * @return
     */
    private static boolean isSpecialParameter(Class<?> clazz) {
        return RowBounds.class.isAssignableFrom(clazz) || ResultHandler.class.isAssignableFrom(clazz);
    }

    /**
     * 获取参数名列表
     * Returns parameter names referenced by SQL providers.
     *
     * @return the names
     */
    public String[] getNames() {
        return names.values().toArray(new String[0]);
    }

    /**
     * 将入参数组进行包装。
     * 包装时会按入参个数，
     * <p>
     * A single non-special parameter is returned without a name. Multiple parameters are named using the naming rule. In
     * addition to the default names, this method also adds the generic names (param1, param2, ...).
     * </p>
     *
     * @param args the args
     * @return the named params
     */
    public Object getNamedParams(Object[] args) {
        final int paramCount = names.size();
        if (args == null || paramCount == 0) {
            return null;
        }
        // 如果不存在 @Param 并且 只有一个参数 ，同时是 collection、Array 时则将其进行包装成 map
        if (!hasParamAnnotation && paramCount == 1) {
            Object value = args[names.firstKey()];
            return wrapToMapIfCollection(value, useActualParamName ? names.get(names.firstKey()) : null);
        } else {
            // 如果有多个参数或指定@Param时，则添加值映射
            final Map<String, Object> param = new ParamMap<>();
            int i = 0;
            for (Map.Entry<Integer, String> entry : names.entrySet()) {
                // 添加 arg0 与值的映射
                param.put(entry.getValue(), args[entry.getKey()]);
                // 添加常规的参数名与值的映射
                final String genericParamName = GENERIC_NAME_PREFIX + (i + 1);
                // 添加 param1 与值的映射，但会进行判断是否包含，防止覆盖 @Param 指定的映射值
                if (!names.containsValue(genericParamName)) {
                    param.put(genericParamName, args[entry.getKey()]);
                }
                i++;
            }
            return param;
        }
    }

    /**
     * Wrap to a {@link ParamMap} if object is {@link Collection} or array.
     *
     * @param object          a parameter object
     * @param actualParamName an actual parameter name (If specify a name, set an object to {@link ParamMap} with specified name)
     * @return a {@link ParamMap}
     * @since 3.5.5
     */
    public static Object wrapToMapIfCollection(Object object, String actualParamName) {
        if (object instanceof Collection) { // 如果是 Collection，则 将其映射为 collection、list
            ParamMap<Object> map = new ParamMap<>();
            map.put("collection", object);
            if (object instanceof List) {
                map.put("list", object);
            }
            // 补充实际参数名的映射，如 param1
            Optional.ofNullable(actualParamName).ifPresent(name -> map.put(name, object));
            return map;
        }
        // 如果是数组，则映射为 array，并且添加 param1
        if (object != null && object.getClass().isArray()) {
            ParamMap<Object> map = new ParamMap<>();
            map.put("array", object);
            Optional.ofNullable(actualParamName).ifPresent(name -> map.put(name, object));
            return map;
        }
        return object;
    }

}
