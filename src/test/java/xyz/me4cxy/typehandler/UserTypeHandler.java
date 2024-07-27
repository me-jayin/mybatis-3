package xyz.me4cxy.typehandler;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import xyz.me4cxy.dto.User;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jayin
 * @since 2024/07/05
 */
public class UserTypeHandler implements TypeHandler<User> {
    @Override
    public void setParameter(PreparedStatement ps, int i, User parameter, JdbcType jdbcType) throws SQLException {

    }

    @Override
    public User getResult(ResultSet rs, String columnName) throws SQLException {
        return null;
    }

    @Override
    public User getResult(ResultSet rs, int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public User getResult(CallableStatement cs, int columnIndex) throws SQLException {
        return null;
    }
}
