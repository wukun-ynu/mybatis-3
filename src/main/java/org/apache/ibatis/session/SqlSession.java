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
package org.apache.ibatis.session;

import java.io.Closeable;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchResult;

/**
 * The primary Java interface for working with MyBatis. Through this interface you can execute commands, get mappers and
 * manage transactions.
 *
 * @author Clinton Begin
 */
public interface SqlSession extends Closeable {

  /**
   * Retrieve a single row mapped from the statement key.
   *
   * @param <T>
   *          the returned object type
   * @param statement
   *          the statement
   *
   * @return Mapped object
   */
  // 泛型方法，参数是要执行查询的sql语句，返回值为查询的结果对象
  <T> T selectOne(String statement);

  /**
   * Retrieve a single row mapped from the statement key and parameter.
   *
   * @param <T>
   *          the returned object type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return Mapped object
   */
  // 第二个参数表示 需要用户传入的实参，即 sql语句绑定的实参
  <T> T selectOne(String statement, Object parameter);

  /**
   * Retrieve a list of mapped objects from the statement key.
   *
   * @param <E>
   *          the returned list element type
   * @param statement
   *          Unique identifier matching the statement to use.
   *
   * @return List of mapped object
   */
  // 查询结果有多条记录，会封装成 结果对象列表 并返回
  <E> List<E> selectList(String statement);

  /**
   * Retrieve a list of mapped objects from the statement key and parameter.
   *
   * @param <E>
   *          the returned list element type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return List of mapped object
   */
  // 参数 + 多记录结果集
  <E> List<E> selectList(String statement, Object parameter);

  /**
   * Retrieve a list of mapped objects from the statement key and parameter, within the specified row bounds.
   *
   * @param <E>
   *          the returned list element type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   * @param rowBounds
   *          Bounds to limit object retrieval
   *
   * @return List of mapped object
   */
  // 参数RowBounds主要用于逻辑分页，逻辑分页会将所有的结果都查询到，
  // 然后根据RowBounds中提供的offset和limit值来获取最后的结果
  <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds);

  /**
   * The selectMap is a special case in that it is designed to convert a list of results into a Map based on one of the
   * properties in the resulting objects. Eg. Return a of Map[Integer,Author] for selectMap("selectAuthors","id")
   *
   * @param <K>
   *          the returned Map keys type
   * @param <V>
   *          the returned Map values type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param mapKey
   *          The property to use as key for each value in the list.
   *
   * @return Map containing key pair data.
   */
  // mapKey表示将结果集中的哪一列（如 主键列或编码列）作为Map的key，
  // value则为列值 对应的那条记录
  <K, V> Map<K, V> selectMap(String statement, String mapKey);

  /**
   * The selectMap is a special case in that it is designed to convert a list of results into a Map based on one of the
   * properties in the resulting objects.
   *
   * @param <K>
   *          the returned Map keys type
   * @param <V>
   *          the returned Map values type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   * @param mapKey
   *          The property to use as key for each value in the list.
   *
   * @return Map containing key pair data.
   */
  // 多了个parameter参数，其它与上面相同
  <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey);

  /**
   * The selectMap is a special case in that it is designed to convert a list of results into a Map based on one of the
   * properties in the resulting objects.
   *
   * @param <K>
   *          the returned Map keys type
   * @param <V>
   *          the returned Map values type
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   * @param mapKey
   *          The property to use as key for each value in the list.
   * @param rowBounds
   *          Bounds to limit object retrieval
   *
   * @return Map containing key pair data.
   */
  // 多了个RowBounds参数，其它与上面相同
  <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds);

  /**
   * A Cursor offers the same results as a List, except it fetches data lazily using an Iterator.
   *
   * @param <T>
   *          the returned cursor element type.
   * @param statement
   *          Unique identifier matching the statement to use.
   *
   * @return Cursor of mapped objects
   */
  // 除了返回值是Cursor对象，其它与selectList相同
  <T> Cursor<T> selectCursor(String statement);

  /**
   * A Cursor offers the same results as a List, except it fetches data lazily using an Iterator.
   *
   * @param <T>
   *          the returned cursor element type.
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return Cursor of mapped objects
   */
  <T> Cursor<T> selectCursor(String statement, Object parameter);

  /**
   * A Cursor offers the same results as a List, except it fetches data lazily using an Iterator.
   *
   * @param <T>
   *          the returned cursor element type.
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   * @param rowBounds
   *          Bounds to limit object retrieval
   *
   * @return Cursor of mapped objects
   */
  <T> Cursor<T> selectCursor(String statement, Object parameter, RowBounds rowBounds);

  /**
   * Retrieve a single row mapped from the statement key and parameter using a {@code ResultHandler}.
   *
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          A parameter object to pass to the statement.
   * @param handler
   *          ResultHandler that will handle each retrieved row
   */
  // 查询出的结果集 将由传入的ResultHandler对象处理，其它与selectList相同
  void select(String statement, Object parameter, ResultHandler handler);

  /**
   * Retrieve a single row mapped from the statement using a {@code ResultHandler}.
   *
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param handler
   *          ResultHandler that will handle each retrieved row
   */
  void select(String statement, ResultHandler handler);

  /**
   * Retrieve a single row mapped from the statement key and parameter using a {@code ResultHandler} and
   * {@code RowBounds}.
   *
   * @param statement
   *          Unique identifier matching the statement to use.
   * @param parameter
   *          the parameter
   * @param rowBounds
   *          RowBound instance to limit the query results
   * @param handler
   *          ResultHandler that will handle each retrieved row
   */
  void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler);

  /**
   * Execute an insert statement.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   *
   * @return int The number of rows affected by the insert.
   */
  // 执行insert语句
  int insert(String statement);

  /**
   * Execute an insert statement with the given parameter object. Any generated autoincrement values or selectKey
   * entries will modify the given parameter object properties. Only the number of rows affected will be returned.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return int The number of rows affected by the insert.
   */
  int insert(String statement, Object parameter);

  /**
   * Execute an update statement. The number of rows affected will be returned.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   *
   * @return int The number of rows affected by the update.
   */
  // 执行update语句
  int update(String statement);

  /**
   * Execute an update statement. The number of rows affected will be returned.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return int The number of rows affected by the update.
   */
  int update(String statement, Object parameter);

  /**
   * Execute a delete statement. The number of rows affected will be returned.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   *
   * @return int The number of rows affected by the delete.
   */
  // 执行delete语句
  int delete(String statement);

  /**
   * Execute a delete statement. The number of rows affected will be returned.
   *
   * @param statement
   *          Unique identifier matching the statement to execute.
   * @param parameter
   *          A parameter object to pass to the statement.
   *
   * @return int The number of rows affected by the delete.
   */
  int delete(String statement, Object parameter);

  /**
   * Flushes batch statements and commits database connection. Note that database connection will not be committed if no
   * updates/deletes/inserts were called. To force the commit call {@link SqlSession#commit(boolean)}
   */
  // 提交事务
  void commit();

  /**
   * Flushes batch statements and commits database connection.
   *
   * @param force
   *          forces connection commit
   */
  void commit(boolean force);

  /**
   * Discards pending batch statements and rolls database connection back. Note that database connection will not be
   * rolled back if no updates/deletes/inserts were called. To force the rollback call
   * {@link SqlSession#rollback(boolean)}
   */
  // 回滚事务
  void rollback();

  /**
   * Discards pending batch statements and rolls database connection back. Note that database connection will not be
   * rolled back if no updates/deletes/inserts were called.
   *
   * @param force
   *          forces connection rollback
   */
  void rollback(boolean force);

  /**
   * Flushes batch statements.
   *
   * @return BatchResult list of updated records
   *
   * @since 3.0.6
   */
  // 将对数据库的操作请求刷到数据库
  List<BatchResult> flushStatements();

  /**
   * Closes the session.
   */
  // 关闭当前session
  @Override
  void close();

  /**
   * Clears local session cache.
   */
  // 清空缓存
  void clearCache();

  /**
   * Retrieves current configuration.
   *
   * @return Configuration
   */
  // 获取Configuration对象
  Configuration getConfiguration();

  /**
   * Retrieves a mapper.
   *
   * @param <T>
   *          the mapper type
   * @param type
   *          Mapper interface class
   *
   * @return a mapper bound to this SqlSession
   */
  // 获取type对应的Mapper对象
  <T> T getMapper(Class<T> type);

  /**
   * Retrieves inner database connection.
   *
   * @return Connection
   */
  // 获取该SqlSession对应的数据库连接
  Connection getConnection();
}
