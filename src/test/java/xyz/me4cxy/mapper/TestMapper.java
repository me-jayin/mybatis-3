package xyz.me4cxy.mapper;

import org.apache.ibatis.annotations.*;
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

    List<Address> selectByCity(@Param("value1") String value1, @Param("value2") String value2);

    @Select("<script>" +
            "SELECT #{city} city <if test=\"city=''\">, 1 AS test</if>" +
            "</script>")
//    @ResultMap("User") // ResultMap 和 @Results 不能同时出现，否则优先使用 @ResultMap 指定的 resultMap
    @Results(value = {
            @Result(property = "city", column = "city")
    })
    List<Address> selectByCity2(String city);

    List<Object> selectMultiToList();

    void selectByIds(@Param("ids") List<String> ids);

//    List<User> selectMultiToMap();
}
