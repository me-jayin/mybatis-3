package xyz.me4cxy.service;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

/**
 * @author jayin
 * @since 2024/05/11
 */
@Scope(value = BeanDefinition.SCOPE_PROTOTYPE, proxyMode = ScopedProxyMode.TARGET_CLASS)
@Service
public class TestService {

    public TestService() {
//        System.out.println("tttt    " + Math.random());
    }

    public void test() {
        System.out.println("in the test method to print the object hash : " + this);
        System.out.println("invoke the test method   " + this);
    }

}
