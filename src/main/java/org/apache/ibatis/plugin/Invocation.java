/*
 *    Copyright 2009-2024 the original author or authors.
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

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.executor.statement.StatementHandler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * 调用信息封装对象，可获取被调用的方法、原对象，以及方法入参，也可以直接执行被调用方法
 *
 * @author Clinton Begin
 */
public class Invocation {

    private static final List<Class<?>> targetClasses = Arrays.asList(Executor.class, ParameterHandler.class,
            ResultSetHandler.class, StatementHandler.class);

    /**
     * 原对象
     */
    private final Object target;
    /**
     * 执行的方法
     */
    private final Method method;
    /**
     * 参数列表
     */
    private final Object[] args;

    public Invocation(Object target, Method method, Object[] args) {
        if (!targetClasses.contains(method.getDeclaringClass())) {
            throw new IllegalArgumentException("Method '" + method + "' is not supported as a plugin target.");
        }
        this.target = target;
        this.method = method;
        this.args = args;
    }

    public Object getTarget() {
        return target;
    }

    public Method getMethod() {
        return method;
    }

    public Object[] getArgs() {
        return args;
    }

    /**
     * 使用原对象、原入参进行调用原方法
     *
     * @return
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public Object proceed() throws InvocationTargetException, IllegalAccessException {
        return method.invoke(target, args);
    }

}
