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
package org.apache.ibatis.binding;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 * @author Kazuki Shimizu
 * MapperMethod 中封装了 Mapper 接口 中对应方法的信息，和对应 sql 语句 的信息，是连接 Mapper 接口 及映射配置文件中定义的 sql 语句 的桥梁。
 * MapperMethod 中持有两个非常重要的属性，这两个属性对应的类 都是 MapperMethod 中的静态内部类。另外，MapperMethod 在被实例化时就对这两个属性进行了初始化。
 */
public class MapperMethod {

  // 两个静态内部类
  private final SqlCommand command;
  private final MethodSignature method;

  public MapperMethod(Class<?> mapperInterface, Method method, Configuration config) {
    this.command = new SqlCommand(config, mapperInterface, method);
    this.method = new MethodSignature(config, mapperInterface, method);
  }

  public Object execute(SqlSession sqlSession, Object[] args) {
    Object result;
    // 根据sql语句的类型，调用sqlSession对应的方法
    switch (command.getType()) {
      case INSERT: {
        // 使用ParamNameResolver处理args实参列表，将用户传入的实参与指定的参数名称关联起来
        Object param = method.convertArgsToSqlCommandParam(args);
        // 获取返回结果，rowCountResult()方法会根据method属性中的returnType
        // 对结果的类型进行转换
        result = rowCountResult(sqlSession.insert(command.getName(), param));
        break;
      }
      case UPDATE: {
        Object param = method.convertArgsToSqlCommandParam(args);
        result = rowCountResult(sqlSession.update(command.getName(), param));
        break;
      }
      case DELETE: {
        Object param = method.convertArgsToSqlCommandParam(args);
        result = rowCountResult(sqlSession.delete(command.getName(), param));
        break;
      }
      case SELECT:
        // 处理返回值为void且有ResultSet通过ResultHandler处理的方法
        if (method.returnsVoid() && method.hasResultHandler()) {
          executeWithResultHandler(sqlSession, args);
          result = null;
          // 处理返回值为集合或数组的方法
        } else if (method.returnsMany()) {
          result = executeForMany(sqlSession, args);
          // 处理返回值为Map的方法
        } else if (method.returnsMap()) {
          result = executeForMap(sqlSession, args);
          // 处理返回值为Cursor的方法
        } else if (method.returnsCursor()) {
          result = executeForCursor(sqlSession, args);
        } else {
          // 处理返回值为单一对象的方法
          Object param = method.convertArgsToSqlCommandParam(args);
          result = sqlSession.selectOne(command.getName(), param);
          // 处理返回值为Optional的方法
          if (method.returnsOptional() && (result == null || !method.getReturnType().equals(result.getClass()))) {
            result = Optional.ofNullable(result);
          }
        }
        break;
      case FLUSH:
        result = sqlSession.flushStatements();
        break;
      default:
        throw new BindingException("Unknown execution method for: " + command.getName());
    }
    if (result == null && method.getReturnType().isPrimitive() && !method.returnsVoid()) {
      throw new BindingException("Mapper method '" + command.getName()
          + "' attempted to return null from a method with a primitive return type (" + method.getReturnType() + ").");
    }
    return result;
  }

  /**
   * 当执行 insert、update、delete 类型的 sql语句 时，其执行结果都要经过本方法处理
   */
  private Object rowCountResult(int rowCount) {
    final Object result;
    // 如果返回值为void，则返回null
    if (method.returnsVoid()) {
      result = null;
      // 如果返回值为Integer，则返回rowCount
    } else if (Integer.class.equals(method.getReturnType()) || Integer.TYPE.equals(method.getReturnType())) {
      result = rowCount;
      // 如果返回值为Long，则返回rowCount
    } else if (Long.class.equals(method.getReturnType()) || Long.TYPE.equals(method.getReturnType())) {
      result = (long) rowCount;
      // 如果返回值为Boolean，则返回rowCount > 0
    } else if (Boolean.class.equals(method.getReturnType()) || Boolean.TYPE.equals(method.getReturnType())) {
      result = rowCount > 0;
    } else {
      throw new BindingException(
          "Mapper method '" + command.getName() + "' has an unsupported return type: " + method.getReturnType());
    }
    return result;
  }

  /**
   * 如果Mapper接口中定义的方法准备使用ResultHandler处理查询结果集，则通过此方法处理
   */
  private void executeWithResultHandler(SqlSession sqlSession, Object[] args) {
    // 获取sql语句对应的MappedStatement对象，该对象中记录了sql语句相关信息
    MappedStatement ms = sqlSession.getConfiguration().getMappedStatement(command.getName());
    //  当使用 ResultHandler 处理结果集时，必须指定 ResultMap 或 ResultType
    if (!StatementType.CALLABLE.equals(ms.getStatementType())
        && void.class.equals(ms.getResultMaps().get(0).getType())) {
      throw new BindingException(
          "method " + command.getName() + " needs either a @ResultMap annotation, a @ResultType annotation,"
              + " or a resultType attribute in XML so a ResultHandler can be used as a parameter.");
    }
    // 转换实参列表
    Object param = method.convertArgsToSqlCommandParam(args);
    // 如果实参列表中有RowBounds，则调用sqlSession的select方法
    if (method.hasRowBounds()) {
      // 从args参数列表中获取RowBounds对象
      RowBounds rowBounds = method.extractRowBounds(args);
      // 执行查询，并用指定的ResultHandler处理结果对象
      sqlSession.select(command.getName(), param, rowBounds, method.extractResultHandler(args));
    } else {
      sqlSession.select(command.getName(), param, method.extractResultHandler(args));
    }
  }

  /**
   * 如果 Mapper接口 中对应方法的返回值为集合(Collection接口实现类) 或 数组，
   * 则调用本方法将结果集处理成 相应的集合或数组
   */
  private <E> Object executeForMany(SqlSession sqlSession, Object[] args) {
    List<E> result;
    // 参数列表转换
    Object param = method.convertArgsToSqlCommandParam(args);
    // 参数列表中是否有 RowBounds类型的参数
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      // 这里使用了 selectList()方法 进行查询，所以返回的结果集就是 List类型的
      result = sqlSession.selectList(command.getName(), param, rowBounds);
    } else {
      result = sqlSession.selectList(command.getName(), param);
    }
    // issue #510 Collections & arrays support
    // 将结果集转换为数组或 Collection集合
    if (!method.getReturnType().isAssignableFrom(result.getClass())) {
      if (method.getReturnType().isArray()) {
        return convertToArray(result);
      }
      return convertToDeclaredCollection(sqlSession.getConfiguration(), result);
    }
    return result;
  }

  /**
   * 本方法与上面的 executeForMap()方法 类似，只不过 sqlSession 调用的是 selectCursor()
   */
  private <T> Cursor<T> executeForCursor(SqlSession sqlSession, Object[] args) {
    Cursor<T> result;
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      result = sqlSession.selectCursor(command.getName(), param, rowBounds);
    } else {
      result = sqlSession.selectCursor(command.getName(), param);
    }
    return result;
  }

  /**
   * 将结果集转换成 Collection集合
   */
  private <E> Object convertToDeclaredCollection(Configuration config, List<E> list) {
    // 通过反射方式创建集合对象
    Object collection = config.getObjectFactory().create(method.getReturnType());
    MetaObject metaObject = config.newMetaObject(collection);
    // 实际上就是调用了Collectoin的addAll()方法
    metaObject.addAll(list);
    return collection;
  }

  /**
   * 本方法和上面的 convertToDeclaredCollection()功能 类似，主要负责将结果对象转换成数组
   */
  @SuppressWarnings("unchecked")
  private <E> Object convertToArray(List<E> list) {
    // 获取数组中元素的类型class
    Class<?> arrayComponentType = method.getReturnType().getComponentType();
    // 根据元素类型和元素数量初始化数组
    Object array = Array.newInstance(arrayComponentType, list.size());
    // 将list转换成数组
    if (!arrayComponentType.isPrimitive()) {
      return list.toArray((E[]) array);
    }
    for (int i = 0; i < list.size(); i++) {
      Array.set(array, i, list.get(i));
    }
    return array;
  }

  /**
   * 如果 Mapper接口 中对应方法的返回值为类型为 Map，则调用此方法执行 sql语句
   */
  private <K, V> Map<K, V> executeForMap(SqlSession sqlSession, Object[] args) {
    Map<K, V> result;
    // 转换实参列表
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      RowBounds rowBounds = method.extractRowBounds(args);
      // 注意这里调用的是 SqlSession 的 selectMap()方法，返回的是一个 Map类型结果集
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey(), rowBounds);
    } else {
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey());
    }
    return result;
  }

  public static class ParamMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -2212268410512043556L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + keySet());
      }
      return super.get(key);
    }

  }

  /**
   * 核心内容: sql id , Sql 类型
   */
  public static class SqlCommand {

    // sql语句的id
    private final String name;
    // sql语句的类型，SqlCommandType是枚举类型，持有常用的增删改查等操作类型
    private final SqlCommandType type;

    public SqlCommand(Configuration configuration, Class<?> mapperInterface, Method method) {
      // 方法名
      final String methodName = method.getName();
      // 该方法对应的类Class对象
      final Class<?> declaringClass = method.getDeclaringClass();
      // MapperStatement封装了sql语句相关的信息，在Mybatis初始化时创建
      MappedStatement ms = resolveMappedStatement(mapperInterface, methodName, declaringClass, configuration);
      if (ms == null) {
        // 处理Flush注解
        if (method.getAnnotation(Flush.class) == null) {
          throw new BindingException(
              "Invalid bound statement (not found): " + mapperInterface.getName() + "." + methodName);
        }
        name = null;
        type = SqlCommandType.FLUSH;
      } else {
        // 初始化name 和type
        name = ms.getId();
        type = ms.getSqlCommandType();
        if (type == SqlCommandType.UNKNOWN) {
          throw new BindingException("Unknown execution method for: " + name);
        }
      }
    }

    public String getName() {
      return name;
    }

    public SqlCommandType getType() {
      return type;
    }

    private MappedStatement resolveMappedStatement(Class<?> mapperInterface, String methodName, Class<?> declaringClass,
        Configuration configuration) {
      // sql语句 的名称默认是由 Mapper接口方法 的 包名.类名.方法名
      String statementId = mapperInterface.getName() + "." + methodName;
      // 检测是否有该名称的sql语句
      if (configuration.hasStatement(statementId)) {
        // 从configuration的mappedStatements容器中获取statementId对应的MappedStatement对象
        return configuration.getMappedStatement(statementId);
      }
      // 如果此方法不是mapperInterface接口定义的，则返回空
      if (mapperInterface.equals(declaringClass)) {
        return null;
      }
      // 对mapperInterface的父接口进行递归处理
      for (Class<?> superInterface : mapperInterface.getInterfaces()) {
        if (declaringClass.isAssignableFrom(superInterface)) {
          MappedStatement ms = resolveMappedStatement(superInterface, methodName, declaringClass, configuration);
          if (ms != null) {
            return ms;
          }
        }
      }
      return null;
    }
  }

  // 方法签名
  public static class MethodSignature {

    // 返回值类型是否为 集合 或 数组
    private final boolean returnsMany;
    // 返回值类型是否为 Map类型
    private final boolean returnsMap;
    // 返回值类型是否为 void
    private final boolean returnsVoid;
    // 返回值类型是否为 Cursor
    private final boolean returnsCursor;
    // 返回值类型是否为 Optional
    private final boolean returnsOptional;
    // 返回值类型的 Class对象
    private final Class<?> returnType;
    // 如果返回值类型为 Map，则用该字段记录了作为 key 的列名
    private final String mapKey;
    // 标记该方法参数列表中 ResultHandler类型参数 的位置
    private final Integer resultHandlerIndex;
    // 标记该方法参数列表中 RowBounds类型参数 的位置
    private final Integer rowBoundsIndex;
    /**
     * 顾名思义，这是一个处理 Mapper接口 中 方法参数列表的解析器，它使用了一个 SortedMap<Integer, String>
     * 类型的容器，记录了参数在参数列表中的位置索引 与 参数名之间的对应关系，key参数 在参数列表中的索引位置，
     * value参数名(参数名可用@Param注解指定，默认使用参数索引作为其名称)
     */
    private final ParamNameResolver paramNameResolver;

    /**
     * MethodSignature 的构造方法会解析对应的 method，并初始化上述字段
     */
    public MethodSignature(Configuration configuration, Class<?> mapperInterface, Method method) {
      // 获取method方法的返回值类型
      Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, mapperInterface);
      if (resolvedReturnType instanceof Class<?>) {
        this.returnType = (Class<?>) resolvedReturnType;
      } else if (resolvedReturnType instanceof ParameterizedType) {
        this.returnType = (Class<?>) ((ParameterizedType) resolvedReturnType).getRawType();
      } else {
        this.returnType = method.getReturnType();
      }
      // 对 MethodSignature 持有的各属性 进行初始化
      this.returnsVoid = void.class.equals(this.returnType);
      this.returnsMany = configuration.getObjectFactory().isCollection(this.returnType) || this.returnType.isArray();
      this.returnsCursor = Cursor.class.equals(this.returnType);
      this.returnsOptional = Optional.class.equals(this.returnType);
      this.mapKey = getMapKey(method);
      this.returnsMap = this.mapKey != null;
      this.rowBoundsIndex = getUniqueParamIndex(method, RowBounds.class);
      this.resultHandlerIndex = getUniqueParamIndex(method, ResultHandler.class);
      this.paramNameResolver = new ParamNameResolver(configuration, method);
    }

    /**
     * 方法主要是把方法参数转换为SQL命令参数。
     *
     * @param args
     * @return
     */
    public Object convertArgsToSqlCommandParam(Object[] args) {
      return paramNameResolver.getNamedParams(args);
    }

    /**
     * 是否有 {@link RowBounds}
     *
     * @return
     */
    public boolean hasRowBounds() {
      return rowBoundsIndex != null;
    }

    public RowBounds extractRowBounds(Object[] args) {
      return hasRowBounds() ? (RowBounds) args[rowBoundsIndex] : null;
    }

    /**
     * 是否有resultHandler
     *
     * @return
     */
    public boolean hasResultHandler() {
      return resultHandlerIndex != null;
    }

    public ResultHandler extractResultHandler(Object[] args) {
      return hasResultHandler() ? (ResultHandler) args[resultHandlerIndex] : null;
    }

    public Class<?> getReturnType() {
      return returnType;
    }

    public boolean returnsMany() {
      return returnsMany;
    }

    public boolean returnsMap() {
      return returnsMap;
    }

    public boolean returnsVoid() {
      return returnsVoid;
    }

    public boolean returnsCursor() {
      return returnsCursor;
    }

    /**
     * return whether return type is {@code java.util.Optional}.
     *
     * @return return {@code true}, if return type is {@code java.util.Optional}
     *
     * @since 3.5.0
     */
    public boolean returnsOptional() {
      return returnsOptional;
    }

    /**
     * 查找指定类型的参数在参数列表中的位置，要查找的参数类型在参数列表中必须是唯一的
     * 如果参数类表中存在多个要查找的类型，则会抛出异常
     */
    private Integer getUniqueParamIndex(Method method, Class<?> paramType) {
      Integer index = null;
      // 获取参数类型
      final Class<?>[] argTypes = method.getParameterTypes();
      for (int i = 0; i < argTypes.length; i++) {
        if (paramType.isAssignableFrom(argTypes[i])) {
          if (index != null) {
            throw new BindingException(
                method.getName() + " cannot have multiple " + paramType.getSimpleName() + " parameters");
          }
          index = i;
        }
      }
      return index;
    }

    public String getMapKey() {
      return mapKey;
    }

    /**
     * 获取 {@link MapKey} 注解数据
     *
     * @param method
     * @return
     */
    private String getMapKey(Method method) {
      String mapKey = null;
      if (Map.class.isAssignableFrom(method.getReturnType())) {
        final MapKey mapKeyAnnotation = method.getAnnotation(MapKey.class);
        if (mapKeyAnnotation != null) {
          mapKey = mapKeyAnnotation.value();
        }
      }
      return mapKey;
    }
  }

}
