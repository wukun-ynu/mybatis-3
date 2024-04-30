## 反射工具箱和TypeHandler
### 反射工具包 Reflector
1. Reflector 类 主要实现了对 JavaBean 的元数据属性的封装，比如：可读属性列表，可写属性列表；及反射操作的封装，如：属性对应的 setter 方法，getter 方法 的反射调用
2. ReflectorFactory通过标识获取对象的方法
3. ObjectFactory 该类也是接口加一个默认实现类，并且支持自定义扩展，Mybatis 中有很多这样的设计方式
### 类型转换
> 类型转换是实现 ORM 的重要一环，由于数据库中的数据类型与 Java 语言 的数据类型并不对等，所以在 PrepareStatement 为 sql 语句 绑定参数时，需要从 Java 类型 转换成 JDBC 类型，而从结果集获取数据时，又要将 JDBC 类型 转换成 Java 类型，Mybatis 使用 TypeHandler 完成了上述的双向转换
1. JdbcType
2. TypeHandler 是 Mybatis 中所有类型转换器的顶层接口，主要用于实现数据从 Java 类型 到 JdbcType 类型 的相互转换
3. TypeHandlerRegistry 
4. 除了 Mabatis 本身自带的 TypeHandler 实现，我们还可以添加自定义的 TypeHandler 实现类，在配置文件 mybatis-config.xml 中的 <typeHandler> 标签下配置好 自定义 TypeHandler，Mybatis 就会在初始化时解析该标签内容，完成 自定义 TypeHandler 的注册。

## DataSource 及 Transaction 模块
1. DataSourceFactory，UnpooledDataSourceFactory 实现了该接口，PooledDataSourceFactory 又继承了 UnpooledDataSourceFactory
2. UnpooledDataSource PooledDataSource-PooledConnection、PoolState、PooledDataSource
> 1.连接池初始化时创建一定数量的连接，并添加到连接池中备用；
2.当程序需要使用数据库连接时，从连接池中请求，用完后会将其返还给连接池，而不是直接关闭；
3.连接池会控制总连接上限及空闲连接上线，如果连接池中的连接总数已达上限，且都被占用，后续的连接请求会短暂阻塞后重新尝试获取连接，如此循环，直到有连接可用；
4.如果连接池中空闲连接较多，已达到空闲连接上限，则返回的连接会被关闭掉，以降低系统开销。
3. PooledDataSource 管理的数据库连接对象 是由其持有的 UnpooledDataSource 对象 创建的，并由 PoolState 管理所有连接的状态。 PooledDataSource 的 getConnection()方法 会首先调用 popConnection()方法 获取 PooledConnection 对象，然后通过 PooledConnection 的 getProxyConnection()方法 获取数据库连接的代理对象。popConnection()方法 是 PooledDataSource 的核心逻辑之一，其整体的逻辑关系
4. Transaction一般我们并不会使用 Mybatis 管理事务，而是将 Mybatis 集成到 Spring，由 Spring 进行事务的管理
5. JdbcTransaction、ManagedTransaction JdbcTransactionFactory、ManagedTransactionFactory

## binding模块
1. MapperRegistry 和 MapperProxyFactory
> MapperRegistry 是 Mapper 接口 及其对应的代理对象工厂的注册中心。Configuration 是 Mybatis 中全局性的配置对象，根据 Mybatis 的核心配置文件 mybatis-config.xml 解析而成。Configuration 通过 mapperRegistry 属性 持有该对象。
Mybatis 在初始化过程中会读取映射配置文件和 Mapper 接口 中的注解信息，并调用 MapperRegistry 的 addMappers()方法 填充 knownMappers 集合，在需要执行某 sql 语句 时，会先调用 getMapper()方法 获取实现了 Mapper 接口 的动态代理对象。
2. MapperProxy实现了 InvocationHandler 接口，为 Mapper 接口 的方法调用织入了统一处理。
3. MapperMethod

## 缓存
> MyBatis 中的缓存分为一级缓存、二级缓存，但在本质上是相同的，它们使用的都是 Cache 接口 的实现。MyBatis 缓存模块 的设计，使用了装饰器模式
1. Cache
2. PerpetualCache 提供了 Cache 接口 的基本实现
3. BlockingCache 是阻塞版本的缓存装饰器，它会保证只有一个线程到数据库中查找指定 key 对应的数据。
4. FifoCache 和 LruCache 缓存清除策略
> FifoCache 是先入先出版本的装饰器，当向缓存添加数据时，如果缓存项的个数已经达到上限，则会将缓存中最老（即最早进入缓存）的缓存项删除
5. SoftCache 是软引用版本的装饰器，当缓存项的个数达到上限时，会先将缓存中最老的缓存项删除，然后使用 JVM 的垃圾回收机制回收缓存中最老的缓存项所占用的内存。
6. CacheKey
> 在 Cache 中唯一确定一个缓存项，需要使用缓存项的 key 进行比较，MyBatis 中因为涉及 动态 SQL 等多方面因素， 其缓存项的 key 不能仅仅通过一个 String 表示，所以 MyBatis 提供了 CacheKey 类 来表示缓存项的 key，
> 在一个 CacheKey 对象 中可以封装多个影响缓存项的因素。 CacheKey 中可以添加多个对象，由这些对象共同确定两个 CacheKey 对象 是否相同。


