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
package org.apache.ibatis.executor.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author Clinton Begin
 */
public class PreparedStatementHandler extends BaseStatementHandler {

  // 构造方法主要用于属性的初始化
  public PreparedStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameter,
      RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    super(executor, mappedStatement, parameter, rowBounds, resultHandler, boundSql);
  }

  @Override
  public int update(Statement statement) throws SQLException {
    PreparedStatement ps = (PreparedStatement) statement;
    ps.execute();
    int rows = ps.getUpdateCount();
    Object parameterObject = boundSql.getParameterObject();
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    keyGenerator.processAfter(executor, mappedStatement, ps, parameterObject);
    return rows;
  }

  @Override
  public void batch(Statement statement) throws SQLException {
    PreparedStatement ps = (PreparedStatement) statement;
    ps.addBatch();
  }

  // 下面的这些方法，除了多了一步 将Statement对象强转成PreparedStatement对象
  // 其它的几乎与SimpleStatementHandler一样
  @Override
  public <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException {
    PreparedStatement ps = (PreparedStatement) statement;
    ps.execute();
    return resultSetHandler.handleResultSets(ps);
  }

  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    PreparedStatement ps = (PreparedStatement) statement;
    ps.execute();
    return resultSetHandler.handleCursorResultSets(ps);
  }

  @Override
  protected Statement instantiateStatement(Connection connection) throws SQLException {
    // 获取SQL语句
    String sql = boundSql.getSql();
    // 根据mappedStatement持有的KeyGenerator的类型进行不同的处理
    if (mappedStatement.getKeyGenerator() instanceof Jdbc3KeyGenerator) {
      // 获取主键列
      String[] keyColumnNames = mappedStatement.getKeyColumns();
      if (keyColumnNames == null) {
        // 返回数据库生成的主键
        return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      } else {
        // 在insert语句执行完成后，会将keyColumnNames指定的列返回
        return connection.prepareStatement(sql, keyColumnNames);
      }
    }
    if (mappedStatement.getResultSetType() == ResultSetType.DEFAULT) {
      // 如果结果集类型是DEFAULT默认的，则直接通过connection获取PreparedStatement对象
      return connection.prepareStatement(sql);
    } else {
      // 否则，设置结果集类型，设置结果集为只读
      return connection.prepareStatement(sql, mappedStatement.getResultSetType().getValue(),
          ResultSet.CONCUR_READ_ONLY);
    }
  }

  // 因为是PrepareStatement对象，所以需要处理占位符"?"
  // 使用了前面介绍的ParameterHandler组件完成
  @Override
  public void parameterize(Statement statement) throws SQLException {
    parameterHandler.setParameters((PreparedStatement) statement);
  }

}
