package xyz.me4cxy.typehandler;

import com.alibaba.fastjson2.JSON;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedJdbcTypes;
import org.apache.ibatis.type.TypeHandler;
import xyz.me4cxy.dto.User;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jayin
 * @since 2024/05/16
 */
@MappedJdbcTypes(JdbcType.VARCHAR)
public class CustomJsonTypeHandler implements TypeHandler<Object> {

    private Class javaType;
    public CustomJsonTypeHandler(Class javaType) {
        this.javaType = javaType;
    }

    @Override
    public void setParameter(PreparedStatement ps, int i, Object parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, JSON.toJSONString(parameter));
    }

    @Override
    public Object getResult(ResultSet rs, String columnName) throws SQLException {
        return JSON.parseObject(rs.getString(columnName), javaType);
    }

    @Override
    public User getResult(ResultSet rs, int columnIndex) throws SQLException {
        return (User) JSON.parseObject(rs.getString(columnIndex), javaType);
    }

    @Override
    public User getResult(CallableStatement cs, int columnIndex) throws SQLException {
        return null;
    }
}
