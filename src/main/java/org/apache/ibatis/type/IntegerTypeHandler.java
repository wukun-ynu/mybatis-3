/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.type;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Clinton Begin
 */
public class IntegerTypeHandler extends BaseTypeHandler<Integer> {

  // NonNull 就是NoneNull，非空的意思
  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, Integer parameter, JdbcType jdbcType)
      throws SQLException {
    // IntegerTypeHandler就调用PreparedStatement的setInt()方法
    // 其它的基本数据类型，以此类推
    ps.setInt(i, parameter);
  }

  @Override
  public Integer getNullableResult(ResultSet rs, String columnName) throws SQLException {
    int result = rs.getInt(columnName);
    return result == 0 && rs.wasNull() ? null : result;
  }

  @Override
  public Integer getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    int result = rs.getInt(columnIndex);
    return result == 0 && rs.wasNull() ? null : result;
  }

  @Override
  public Integer getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    int result = cs.getInt(columnIndex);
    return result == 0 && cs.wasNull() ? null : result;
  }
}
