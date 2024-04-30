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
package org.apache.ibatis.datasource.pooled;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.ibatis.reflection.ExceptionUtil;

/**
 * PooledDataSource 通过管理 PooledConnection 来实现对 java.sql.Connection 的管理。PooledConnection 封装了 java.sql.Connection 数据库连接对象 及其代理对象（JDK 动态代理生成的）。
 * PooledConnection 继承了 JDK 动态代理 的 InvocationHandler 接口。
 */
class PooledConnection implements InvocationHandler {

  private static final String CLOSE = "close";
  private static final Class<?>[] IFACES = { Connection.class };

  private final int hashCode;
  // 记录当前PooledConnection对象，所属的PooledDataSource对象
  // 当调用close()方法时会将PooledConnection放回该PooledDataSource
  private final PooledDataSource dataSource;
  // 真正的数据库连接对象
  private final Connection realConnection;
  // 代理连接对象
  private final Connection proxyConnection;
  // 从连接池中取出该连接时的时间戳
  private long checkoutTimestamp;
  // 创建该连接时的时间戳
  private long createdTimestamp;
  // 最后一次使用的时间戳
  private long lastUsedTimestamp;
  // 由数据库URL、用户名、密码计算出来的Hash值，可用于标识该连接所在的连接池
  private int connectionTypeCode;
  // 检测当前的PooledConnection连接池连接对象是否有效，主要用于防止程序通过close()方法将
  // 连接还给连接池之后，依然通过该连接池操作数据库
  private boolean valid;

  /**
   * Constructor for SimplePooledConnection that uses the Connection and PooledDataSource passed in.
   *
   * @param connection
   *          - the connection that is to be presented as a pooled connection
   * @param dataSource
   *          - the dataSource that the connection is from
   */
  public PooledConnection(Connection connection, PooledDataSource dataSource) {
    this.hashCode = connection.hashCode();
    this.realConnection = connection;
    this.dataSource = dataSource;
    this.createdTimestamp = System.currentTimeMillis();
    this.lastUsedTimestamp = System.currentTimeMillis();
    this.valid = true;
    this.proxyConnection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), IFACES, this);
  }

  /**
   * Invalidates the connection.
   */
  public void invalidate() {
    valid = false;
  }

  /**
   * Method to see if the connection is usable.
   *
   * @return True if the connection is usable
   */
  public boolean isValid() {
    return valid && realConnection != null && dataSource.pingConnection(this);
  }

  /**
   * Getter for the *real* connection that this wraps.
   *
   * @return The connection
   */
  public Connection getRealConnection() {
    return realConnection;
  }

  /**
   * Getter for the proxy for the connection.
   *
   * @return The proxy
   */
  public Connection getProxyConnection() {
    return proxyConnection;
  }

  /**
   * Gets the hashcode of the real connection (or 0 if it is null).
   *
   * @return The hashcode of the real connection (or 0 if it is null)
   */
  public int getRealHashCode() {
    return realConnection == null ? 0 : realConnection.hashCode();
  }

  /**
   * Getter for the connection type (based on url + user + password).
   *
   * @return The connection type
   */
  public int getConnectionTypeCode() {
    return connectionTypeCode;
  }

  /**
   * Setter for the connection type.
   *
   * @param connectionTypeCode
   *          - the connection type
   */
  public void setConnectionTypeCode(int connectionTypeCode) {
    this.connectionTypeCode = connectionTypeCode;
  }

  /**
   * Getter for the time that the connection was created.
   *
   * @return The creation timestamp
   */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /**
   * Setter for the time that the connection was created.
   *
   * @param createdTimestamp
   *          - the timestamp
   */
  public void setCreatedTimestamp(long createdTimestamp) {
    this.createdTimestamp = createdTimestamp;
  }

  /**
   * Getter for the time that the connection was last used.
   *
   * @return - the timestamp
   */
  public long getLastUsedTimestamp() {
    return lastUsedTimestamp;
  }

  /**
   * Setter for the time that the connection was last used.
   *
   * @param lastUsedTimestamp
   *          - the timestamp
   */
  public void setLastUsedTimestamp(long lastUsedTimestamp) {
    this.lastUsedTimestamp = lastUsedTimestamp;
  }

  /**
   * Getter for the time since this connection was last used.
   *
   * @return - the time since the last use
   */
  public long getTimeElapsedSinceLastUse() {
    return System.currentTimeMillis() - lastUsedTimestamp;
  }

  /**
   * Getter for the age of the connection.
   *
   * @return the age
   */
  public long getAge() {
    return System.currentTimeMillis() - createdTimestamp;
  }

  /**
   * Getter for the timestamp that this connection was checked out.
   *
   * @return the timestamp
   */
  public long getCheckoutTimestamp() {
    return checkoutTimestamp;
  }

  /**
   * Setter for the timestamp that this connection was checked out.
   *
   * @param timestamp
   *          the timestamp
   */
  public void setCheckoutTimestamp(long timestamp) {
    this.checkoutTimestamp = timestamp;
  }

  /**
   * Getter for the time that this connection has been checked out.
   *
   * @return the time
   */
  public long getCheckoutTime() {
    return System.currentTimeMillis() - checkoutTimestamp;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Allows comparing this connection to another.
   *
   * @param obj
   *          - the other connection to test for equality
   *
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PooledConnection) {
      return realConnection.hashCode() == ((PooledConnection) obj).realConnection.hashCode();
    }
    if (obj instanceof Connection) {
      return hashCode == obj.hashCode();
    } else {
      return false;
    }
  }

  /**
   * Required for InvocationHandler implementation.
   *
   * @param proxy
   *          - not used
   * @param method
   *          - the method to be executed
   * @param args
   *          - the parameters to be passed to the method
   *
   * @see java.lang.reflect.InvocationHandler#invoke(Object, java.lang.reflect.Method, Object[])
   */
  /**
   * invoke()方法 是本类的重点实现，也是 proxyConnection代理连接对象 的代理逻辑实现
   * 它会对 close()方法 的调用进行处理，并在调用 realConnection对象 的方法之前进行校验
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    // 如果调用的是close()方法，则将其放进连接池，而不是真正的关闭连接
    if (CLOSE.equals(methodName)) {
      dataSource.pushConnection(this);
      return null;
    }
    try {
      if (!Object.class.equals(method.getDeclaringClass())) {
        // issue #579 toString() should never fail
        // throw an SQLException instead of a Runtime
        // 通过上面的valid字段 校验连接是否有效
        checkConnection();
      }
      // 调用realConnection对象的对应方法
      return method.invoke(realConnection, args);
    } catch (Throwable t) {
      throw ExceptionUtil.unwrapThrowable(t);
    }

  }

  private void checkConnection() throws SQLException {
    if (!valid) {
      throw new SQLException("Error accessing PooledConnection. Connection is invalid.");
    }
  }

}
