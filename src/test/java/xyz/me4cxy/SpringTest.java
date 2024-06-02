package xyz.me4cxy;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import xyz.me4cxy.controller.TestController;
import xyz.me4cxy.service.TestService;

import javax.annotation.Resource;

/**
 * @author jayin
 * @since 2024/05/11
 */
@MapperScan(basePackages = "xyz.me4cxy.mapper")
@SpringBootApplication
public class SpringTest {
    @Resource
    private TestService testService;
    @Resource
    private TestController testController;

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(SpringTest.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }


}
