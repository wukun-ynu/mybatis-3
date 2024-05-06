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
package org.apache.ibatis.executor;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * @author Jeff Butler
 */
public class BatchExecutor extends BaseExecutor {

  public static final int BATCH_UPDATE_RETURN_VALUE = Integer.MIN_VALUE + 1002;
  // 缓存多个Statement对象，其中每个Statement对象中都可以缓存多条
  // 结构相同 但参数不同的sql语句
  private final List<Statement> statementList = new ArrayList<>();
  // 记录批处理的结果，BatchResult中通过updateCounts字段
  // 记录每个Statement对象 执行批处理的结果
  private final List<BatchResult> batchResultList = new ArrayList<>();
  // 记录当前执行的sql语句
  private String currentSql;
  // 记录当前执行的MappedStatement对象
  private MappedStatement currentStatement;

  public BatchExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  /**
   * JDBC中的批处理只支持insert、update、delete等类型的SQL语句，不支持select类型的
   * SQL语句，所以doUpdate()方法是BatchExecutor中最重要的一个方法。
   * 本方法在添加一条SQL语句时，首先会将currentSql字段记录的SQL语句以及currentStatement字段
   * 记录的MappedStatement对象与当前添加的SQL以及MappedStatement对象进行比较，
   * 如果相同则添加到同一个Statement对象中等待执行，如果不同则创建新的Statement对象
   * 井将其缓存到statementList集合中等待执行
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameterObject) throws SQLException {
    // 获取configuration配置对象
    final Configuration configuration = ms.getConfiguration();
    // 实例化一个StatementHandler，并返回
    final StatementHandler handler = configuration.newStatementHandler(this, ms, parameterObject, RowBounds.DEFAULT,
        null, null);
    // 获取需要执行的sql语句
    final BoundSql boundSql = handler.getBoundSql();
    final String sql = boundSql.getSql();
    final Statement stmt;
    // 判断要执行的sql语句结构 及 MappedStatement对象 是否与上次的相同
    if (sql.equals(currentSql) && ms.equals(currentStatement)) {
      // 相同则添加到同一个Statement对象中等待执行
      // 首先获取statementList集合中最后一个Statement对象
      int last = statementList.size() - 1;
      stmt = statementList.get(last);
      // 重新设置事务超时时间
      applyTransactionTimeout(stmt);
      // 绑定实参，处理占位符？
      handler.parameterize(stmt);// fix Issues 322
      // 查找对应的BatchResult对象，并记录用户传入的实参
      BatchResult batchResult = batchResultList.get(last);
      batchResult.addParameterObject(parameterObject);
    } else {
      // 不同则创建新的Statement对象井将其缓存到statementList集合中等待执行
      Connection connection = getConnection(ms.getStatementLog());
      // 创建新的Statement对象
      stmt = handler.prepare(connection, transaction.getTimeout());
      // 绑定实参，处理占位符？
      handler.parameterize(stmt); // fix Issues 322
      currentSql = sql;
      currentStatement = ms;
      // 记录本次的sql语句 及 Statement对象
      statementList.add(stmt);
      // 添加新的BatchResult对象
      batchResultList.add(new BatchResult(ms, sql, parameterObject));
    }
    // 底层通过调用java.sql.Statement的addBatch()方法添加sql语句
    handler.batch(stmt);
    return BATCH_UPDATE_RETURN_VALUE;
  }

  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameterObject, RowBounds rowBounds,
      ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      flushStatements();
      Configuration configuration = ms.getConfiguration();
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameterObject, rowBounds,
          resultHandler, boundSql);
      Connection connection = getConnection(ms.getStatementLog());
      stmt = handler.prepare(connection, transaction.getTimeout());
      handler.parameterize(stmt);
      return handler.query(stmt, resultHandler);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql)
      throws SQLException {
    flushStatements();
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    Connection connection = getConnection(ms.getStatementLog());
    Statement stmt = handler.prepare(connection, transaction.getTimeout());
    handler.parameterize(stmt);
    Cursor<E> cursor = handler.queryCursor(stmt);
    stmt.closeOnCompletion();
    return cursor;
  }

  /**
   * 上面的doUpdate()方法负责添加待执行的sql语句，
   * 而doFlushStatements()方法则将上面添加的sql语句进行批量处理
   */
  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
    try {
      // 用于存储批处理结果的集合
      List<BatchResult> results = new ArrayList<>();
      // 如果要回滚 则返回一个空集合
      if (isRollback) {
        return Collections.emptyList();
      }
      // 批处理statementList集合中的所有Statement对象
      for (int i = 0, n = statementList.size(); i < n; i++) {
        // 获取Statement对象 和其对应的 BatchResult对象
        Statement stmt = statementList.get(i);
        applyTransactionTimeout(stmt);
        BatchResult batchResult = batchResultList.get(i);
        try {
          // 调用Statement对象的executeBatch()方法，批量执行其中记录的sql语句
          // 将执行返回的int[]数组set进batchResult的updateCounts字段，
          // 其中的每一个int值都代表了对应的sql语句 影响的记录条数
          batchResult.setUpdateCounts(stmt.executeBatch());
          MappedStatement ms = batchResult.getMappedStatement();
          List<Object> parameterObjects = batchResult.getParameterObjects();
          // 获取配置的KeyGenerator对象
          KeyGenerator keyGenerator = ms.getKeyGenerator();
          if (Jdbc3KeyGenerator.class.equals(keyGenerator.getClass())) {
            Jdbc3KeyGenerator jdbc3KeyGenerator = (Jdbc3KeyGenerator) keyGenerator;
            // 获取数据库生成的主键 并设置到parameterObjects中
            jdbc3KeyGenerator.processBatch(ms, stmt, parameterObjects);
          } else if (!NoKeyGenerator.class.equals(keyGenerator.getClass())) { // issue #141
            // 对于其它类型的KeyGenerator，则调用其processAfter进行处理
            for (Object parameter : parameterObjects) {
              keyGenerator.processAfter(this, ms, stmt, parameter);
            }
          }
          // Close statement to close cursor #1109
          closeStatement(stmt);
        } catch (BatchUpdateException e) {
          StringBuilder message = new StringBuilder();
          message.append(batchResult.getMappedStatement().getId()).append(" (batch index #").append(i + 1).append(")")
              .append(" failed.");
          if (i > 0) {
            message.append(" ").append(i)
                .append(" prior sub executor(s) completed successfully, but will be rolled back.");
          }
          throw new BatchExecutorException(message.toString(), e, results, batchResult);
        }
        // 添加处理完的BatchResult对象到要返回的List<BatchResult>集合中
        results.add(batchResult);
      }
      return results;
    } finally {
      // 关闭所有的Statement对象
      for (Statement stmt : statementList) {
        closeStatement(stmt);
      }
      // 清空currentSql、statementList、batchResultList对象
      currentSql = null;
      statementList.clear();
      batchResultList.clear();
    }
  }

}
