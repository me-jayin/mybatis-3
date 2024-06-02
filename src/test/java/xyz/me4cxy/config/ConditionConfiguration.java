package xyz.me4cxy.config;

import org.mybatis.spring.boot.autoconfigure.MybatisAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xyz.me4cxy.service.TestService;

import java.util.Date;

/**
 * @author jayin
 * @since 2024/05/11
 */
@AutoConfigureAfter(MybatisAutoConfiguration.MapperScannerRegistrarNotFoundConfiguration.class)
@Configuration
@ConditionalOnMissingBean(TestService.class)
public class ConditionConfiguration {

    public ConditionConfiguration() {
        System.out.println("cccccc");
    }

    @Configuration
    public static class InnerConfiguration {

        @Bean
        public Date date() {
            System.out.println("iiiii");
            return new Date();
        }

    }

}
