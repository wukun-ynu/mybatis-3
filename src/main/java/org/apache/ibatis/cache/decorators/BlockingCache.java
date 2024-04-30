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
package org.apache.ibatis.cache.decorators;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;

/**
 * <p>
 * Simple blocking decorator
 * <p>
 * Simple and inefficient version of EhCache's BlockingCache decorator. It sets a lock over a cache key when the element
 * is not found in cache. This way, other threads will wait until this element is filled instead of hitting the
 * database.
 * <p>
 * By its nature, this implementation can cause deadlock when used incorrectly.
 *
 * @author Eduardo Macarron
 * BlockingCache 是阻塞版本的缓存装饰器，它会保证只有一个线程到数据库中查找指定 key 对应的数据。
 */
public class BlockingCache implements Cache {

  // 阻塞超时时长
  private long timeout;
  // 持有的被装饰者
  private final Cache delegate;
  // 初始化持有的持有的被装饰者和锁集合
  private final ConcurrentHashMap<Object, CountDownLatch> locks;

  public BlockingCache(Cache delegate) {
    this.delegate = delegate;
    this.locks = new ConcurrentHashMap<>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  // 假设 线程 A 从数据库中查找到 keyA 对应的结果对象后，将结果对象放入到 BlockingCache 中，此时 线程 A 会释放 keyA 对应的锁，唤醒阻塞在该锁上的线程。
  // 其它线程即可从 BlockingCache 中获取 keyA 对应的数据，而不是再次访问数据库。
  @Override
  public void putObject(Object key, Object value) {
    try {
      // 存入key和其对应的缓存项
      delegate.putObject(key, value);
    } finally {
      // 最后释放锁
      releaseLock(key);
    }
  }

  @Override
  public Object getObject(Object key) {
    acquireLock(key);
    Object value = delegate.getObject(key);
    if (value != null) {
      releaseLock(key);
    }
    return value;
  }

  @Override
  public Object removeObject(Object key) {
    // despite its name, this method is called only to release locks
    releaseLock(key);
    return null;
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  // 假设 线程 A 在 BlockingCache 中未查找到 keyA 对应的缓存项时，线程 A 会获取 keyA 对应的锁，这样，线程 A 在后续查找 keyA 时，其它线程会被阻塞。
  // 根据key获取锁对象，然后上锁
  private void acquireLock(Object key) {
    CountDownLatch newLatch = new CountDownLatch(1);
    while (true) {
      // Java8 新特性，Map系列类 中新增的方法
      // V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
      // 表示，若 key 对应的 value 为空，则将第二个参数的返回值存入该 Map集合 并返回
      CountDownLatch latch = locks.putIfAbsent(key, newLatch);
      if (latch == null) {
        break;
      }
      try {
        // 获取锁，带超时时长
        if (timeout > 0) {
          boolean acquired = latch.await(timeout, TimeUnit.MILLISECONDS);
          if (!acquired) {
            // 超时，则抛出异常
            throw new CacheException(
                "Couldn't get a lock in " + timeout + " for the key " + key + " at the cache " + delegate.getId());
          }
        } else {
          latch.await();
        }
      } catch (InterruptedException e) {
        // 如果获取锁失败，则阻塞一段时间
        throw new CacheException("Got interrupted while trying to acquire lock for key " + key, e);
      }
    }
  }

  private void releaseLock(Object key) {
    CountDownLatch latch = locks.remove(key);
    // 锁是否被当前线程持有
    if (latch == null) {
      throw new IllegalStateException("Detected an attempt at releasing unacquired lock. This should never happen.");
    }
    // 是，锁释放
    latch.countDown();
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }
}
