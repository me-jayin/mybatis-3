package xyz.me4cxy;

import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.junit.Test;
import xyz.me4cxy.dto.User;

/**
 * @author jayin
 * @since 2024/06/02
 */
public class MetaClassTest {
    @Test
    public void test() {
        MetaClass metaClass = MetaClass.forClass(User.class, new DefaultReflectorFactory());
        Class<?> setterType = metaClass.getSetterType("mainAddr.city");
        System.out.println(setterType);
    }

    @Test
    public void test2() {
        MetaClass metaClass = MetaClass.forClass(User.class, new DefaultReflectorFactory());
        Class<?> type = metaClass.getGetterType("address[1]");
        Class<?> type2 = metaClass.getGetterType("address");
        System.out.println(type);
        System.out.println(type2);
    }

}
