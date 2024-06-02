package xyz.me4cxy.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import xyz.me4cxy.dto.Address;
import xyz.me4cxy.dto.User;

import java.util.List;

/**
 * @author jayin
 * @since 2024/05/11
 */
@Mapper
public interface TestMapper {

    List<User> selectOne();

    List<User> selectOfParamMap(String city, String province);

    List<Address> selectByCity(String value1, String value2);

    @Select("<script>" +
            "SELECT #{city} city <if test=\"city=''\">, 1 AS test</if>" +
            "</script>")
    List<Address> selectByCity2(String city);

    List<Object> selectMultiToList();

//    List<User> selectMultiToMap();
}
