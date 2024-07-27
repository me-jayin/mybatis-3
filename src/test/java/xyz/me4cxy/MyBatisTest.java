package xyz.me4cxy;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.me4cxy.mapper.TestMapper;

/**
 * @author jayin
 * @since 2024/05/14
 */
public class MyBatisTest {
    public static void main(String[] args) throws Exception {
        Logger test = LoggerFactory.getLogger("test");
        test.info("test...");
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(Resources.getResourceAsReader("mybatis-config.xml"));
        System.out.println(factory);

        SqlSession sqlSession = factory.openSession();
        System.out.println(sqlSession);

        TestMapper mapper = sqlSession.getMapper(TestMapper.class);
//        System.out.println(mapper.selectOne());
//        System.out.println(mapper.selectByCity("test1", "test2"));
//        System.out.println(mapper.selectByCity("test1", "test2"));
//        System.out.println(mapper.selectByCity2("test1"));
        System.out.println(mapper.selectMultiToList());
//        System.out.println(mapper.selectMultiToMap());
//        mapper.selectByIds(Arrays.asList("1", "2", "3"));

//        System.out.println(mapper.selectOfParamMap("123", "222"));

//        sqlSession = factory.openSession();
//        mapper = sqlSession.getMapper(TestMapper.class);
//        System.out.println(mapper.selectByCity("test1", "test2"));
    }
}
