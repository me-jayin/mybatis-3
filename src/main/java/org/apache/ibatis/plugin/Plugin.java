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
package org.apache.ibatis.plugin;

import org.apache.ibatis.reflection.ExceptionUtil;
import org.apache.ibatis.util.MapUtil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 对 Interceptor 进行包装，由于拦截器实际上使用 Jdk Proxy 来实现的拦截器功能，因此该类通过 InvocationHandler，
 * 在调用方法时，校验方法是否命中 Signature 指定的描述，来控制被包装的 Interceptor 是否需要执行
 *
 * @author Clinton Begin
 */
public class Plugin implements InvocationHandler {
    /**
     * 被代理的原对象，用于拦截器中执行
     */
    private final Object target;
    /**
     * 当前被包装的拦截器本体，如果满足被拦截条件则会调用该拦截器
     */
    private final Interceptor interceptor;
    /**
     * 拦截签名map
     */
    private final Map<Class<?>, Set<Method>> signatureMap;

    private Plugin(Object target, Interceptor interceptor, Map<Class<?>, Set<Method>> signatureMap) {
        this.target = target;
        this.interceptor = interceptor;
        this.signatureMap = signatureMap;
    }

    /**
     * 对指定对象进行包装，使其具有拦截器能力
     *
     * @param target      目标对象
     * @param interceptor 目标拦截器
     * @return
     */
    public static Object wrap(Object target, Interceptor interceptor) {
        // 获取拦截目标的描述符，如 拦截器生效的目标对象类型、方法及方法入参 等
        Map<Class<?>, Set<Method>> signatureMap = getSignatureMap(interceptor);
        // 获取被代理对象的 class
        Class<?> type = target.getClass();
        // 根据描述符 Signature 指定的类型，获取当前目标对象存在的接口
        Class<?>[] interfaces = getAllInterfaces(type, signatureMap);
        if (interfaces.length > 0) {
            // 基于获取到的 interface + 拦截器 + 需进行拦截的方法签名，生成代理对象
            return Proxy.newProxyInstance(type.getClassLoader(), interfaces, new Plugin(target, interceptor, signatureMap));
        }
        return target;
    }

    /**
     * 代理对象被调用时触发，会判断被调用的方法是否在 被包装拦截器 的 Signature 方法中，如果在则执行拦截器的调用操作
     * @param proxy 当前被调用的代理对象
     * @param method 当前被调用的方法
     * @param args 方法入参
     *
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            // 获取当前被调用方法所在的定义的类
            Set<Method> methods = signatureMap.get(method.getDeclaringClass());
            // 判断方法是否在描述 Signature 中，如果属于描述的方法，则执行拦截器
            if (methods != null && methods.contains(method)) {
                return interceptor.intercept(new Invocation(target, method, args));
            }
            // 否则直接执行方法调用
            return method.invoke(target, args);
        } catch (Exception e) {
            throw ExceptionUtil.unwrapThrowable(e);
        }
    }

    /**
     * 获取拦截器的拦截目标描述符，并将其使用 class-method 的方式映射
     *
     * @param interceptor 当前拦截器
     * @return key：执行器生效的类型，value：执行器生效的方法Method对象
     */
    private static Map<Class<?>, Set<Method>> getSignatureMap(Interceptor interceptor) {
        /*
         * 获取拦截器注解，如
         * @Intercepts(
         *     @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class })
         * )
         * 标识当前拦截器，仅在执行 Executor.update(MappedStatement, Object) 方法时才生效，其他方法则忽略不执行
         */
        Intercepts interceptsAnnotation = interceptor.getClass().getAnnotation(Intercepts.class);
        // issue #251
        if (interceptsAnnotation == null) {
            throw new PluginException(
                    "No @Intercepts annotation was found in interceptor " + interceptor.getClass().getName());
        }
        Signature[] sigs = interceptsAnnotation.value();
        Map<Class<?>, Set<Method>> signatureMap = new HashMap<>();
        for (Signature sig : sigs) {
            Set<Method> methods = MapUtil.computeIfAbsent(signatureMap, sig.type(), k -> new HashSet<>());
            try {
                Method method = sig.type().getMethod(sig.method(), sig.args());
                methods.add(method);
            } catch (NoSuchMethodException e) {
                throw new PluginException("Could not find method on " + sig.type() + " named " + sig.method() + ". Cause: " + e,
                        e);
            }
        }
        return signatureMap;
    }

    /**
     * 获取当前类型中，能够进行拦截操作的接口。因为当前类型可能实现多个接口，即可能会有多个拦截器实现
     *
     * @param type
     * @param signatureMap
     * @return
     */
    private static Class<?>[] getAllInterfaces(Class<?> type, Map<Class<?>, Set<Method>> signatureMap) {
        Set<Class<?>> interfaces = new HashSet<>();
        while (type != null) {
            for (Class<?> c : type.getInterfaces()) {
                if (signatureMap.containsKey(c)) {
                    interfaces.add(c);
                }
            }
            type = type.getSuperclass();
        }
        return interfaces.toArray(new Class<?>[0]);
    }

}
