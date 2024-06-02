package xyz.me4cxy.mapper;

import org.apache.ibatis.annotations.Mapper;
import xyz.me4cxy.dto.User;

import java.util.List;

/**
 * @author jayin
 * @since 2024/05/11
 */
@Mapper
public interface Test2Mapper {

    List<User> selectOne();

}
