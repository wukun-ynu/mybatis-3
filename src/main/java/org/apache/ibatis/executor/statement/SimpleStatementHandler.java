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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @author Clinton Begin
 */
public class SimpleStatementHandler extends BaseStatementHandler {

  // 构造方法主要用于属性的初始化
  public SimpleStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameter,
      RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    super(executor, mappedStatement, parameter, rowBounds, resultHandler, boundSql);
  }

  // 本方法用于执行insert、delete、update等类型的SQL语句，并且会根据配置的
  // KeyGenerator获取数据库生成的主键
  @Override
  public int update(Statement statement) throws SQLException {
    // 获取SQL语句及parameterObject
    String sql = boundSql.getSql();
    Object parameterObject = boundSql.getParameterObject();
    // 获取配置的KeyGenerator数据库主键生成器
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    int rows;
    if (keyGenerator instanceof Jdbc3KeyGenerator) {
      // 执行SQL语句
      statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
      // 获取更新的条数
      rows = statement.getUpdateCount();
      // 将数据库生成的主键添加到parameterObject中
      keyGenerator.processAfter(executor, mappedStatement, statement, parameterObject);
    } else if (keyGenerator instanceof SelectKeyGenerator) {
      // 执行SQL语句
      statement.execute(sql);
      // 获取更新的条数
      rows = statement.getUpdateCount();
      //  执行<selectKey>节点中配置的SQL语句，将从数据库获取到的主键 添加到parameterObject中
      keyGenerator.processAfter(executor, mappedStatement, statement, parameterObject);
    } else {
      statement.execute(sql);
      rows = statement.getUpdateCount();
    }
    return rows;
  }

  // 下面的batch()及queryCursor()方法的实现与上面的query()方法非常类似
  @Override
  public void batch(Statement statement) throws SQLException {
    String sql = boundSql.getSql();
    statement.addBatch(sql);
  }

  @Override
  public <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException {
    String sql = boundSql.getSql();
    statement.execute(sql);
    return resultSetHandler.handleResultSets(statement);
  }

  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    String sql = boundSql.getSql();
    statement.execute(sql);
    return resultSetHandler.handleCursorResultSets(statement);
  }

  // 直接通过Connection创建Statement对象
  @Override
  protected Statement instantiateStatement(Connection connection) throws SQLException {
    if (mappedStatement.getResultSetType() == ResultSetType.DEFAULT) {
      // 如果结果集类型是DEFAULT默认的，则直接用connection创建Statement对象
      return connection.createStatement();
    }
    // 否则，设置结果集类型，设置结果集 只读
    return connection.createStatement(mappedStatement.getResultSetType().getValue(), ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public void parameterize(Statement statement) {
    // N/A
  }

}
