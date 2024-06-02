package xyz.me4cxy.controller;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import xyz.me4cxy.service.TestService;

import javax.annotation.Resource;

/**
 * @author jayin
 * @since 2024/05/12
 */
@Component
public class TestController implements InitializingBean {
    @Resource
    private TestService testService;


    @Override
    public void afterPropertiesSet() throws Exception {
        System.out.println(testService);
        System.out.println(testService);
        testService.test();
    }
}
