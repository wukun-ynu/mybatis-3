/*
 *    Copyright 2009-2024 the original author or authors.
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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ibatis.cache.Cache;

/**
 * Soft Reference cache decorator.
 * <p>
 * Thanks to Dr. Heinz Kabutz for his guidance here.
 *
 * @author Clinton Begin
 */
public class SoftCache implements Cache {
  // 这里使用了 LinkedList 作为容器，在 SoftCache 中，最近使用的一部分缓存项不会被 GC
  // 这是通过将其 value 添加到 hardLinksToAvoidGarbageCollection集合 实现的（即，有强引用指向其value）
  private final Deque<Object> hardLinksToAvoidGarbageCollection;
  // 引用队列，用于记录已经被 GC 的缓存项所对应的 SoftEntry对象
  private final ReferenceQueue<Object> queueOfGarbageCollectedEntries;
  // 持有的被装饰者
  private final Cache delegate;
  // 强连接的个数，默认为256
  private int numberOfHardLinks;
  private final ReentrantLock lock = new ReentrantLock();

  // 构造方法进行属性的初始化
  public SoftCache(Cache delegate) {
    this.delegate = delegate;
    this.numberOfHardLinks = 256;
    this.hardLinksToAvoidGarbageCollection = new LinkedList<>();
    this.queueOfGarbageCollectedEntries = new ReferenceQueue<>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    removeGarbageCollectedItems();
    return delegate.getSize();
  }

  public void setSize(int size) {
    this.numberOfHardLinks = size;
  }

  @Override
  public void putObject(Object key, Object value) {
    // 清除已经被 GC 的缓存项
    removeGarbageCollectedItems();
    // 添加缓存
    delegate.putObject(key, new SoftEntry(key, value, queueOfGarbageCollectedEntries));
  }

  @Override
  public Object getObject(Object key) {
    Object result = null;
    @SuppressWarnings("unchecked") // assumed delegate cache is totally managed by this cache
    // 用一个软引用指向 key 对应的缓存项
    SoftReference<Object> softReference = (SoftReference<Object>) delegate.getObject(key);
    // 检测缓存中是否有对应的缓存项
    if (softReference != null) {
      // 获取 softReference 引用的 value
      result = softReference.get();
      // 如果 softReference 引用的对象已经被 GC，则从缓存中清除对应的缓存项
      if (result == null) {
        delegate.removeObject(key);
      } else {
        // See #586 (and #335) modifications need more than a read lock
        lock.lock();
        try {
          // 将缓存项的 value 添加到 hardLinksToAvoidGarbageCollection集合 中保存
          hardLinksToAvoidGarbageCollection.addFirst(result);
          // 如果 hardLinksToAvoidGarbageCollection 的容积已经超过 numberOfHardLinks
          // 则将最老的缓存项从 hardLinksToAvoidGarbageCollection 中清除，FIFO
          if (hardLinksToAvoidGarbageCollection.size() > numberOfHardLinks) {
            hardLinksToAvoidGarbageCollection.removeLast();
          }
        } finally {
          lock.unlock();
        }
      }
    }
    return result;
  }

  @Override
  public Object removeObject(Object key) {
    // 清除指定的缓存项之前，也会先清理被 GC 的缓存项
    removeGarbageCollectedItems();
    @SuppressWarnings("unchecked")
    SoftReference<Object> softReference = (SoftReference<Object>) delegate.removeObject(key);
    return softReference == null ? null : softReference.get();
  }

  @Override
  public void clear() {
    lock.lock();
    try {
      // 清理强引用集合
      hardLinksToAvoidGarbageCollection.clear();
    } finally {
      lock.unlock();
    }
    // 清理被 GC 的缓存项
    removeGarbageCollectedItems();
    // 清理最底层的缓存项
    delegate.clear();
  }

  private void removeGarbageCollectedItems() {
    SoftEntry sv;
    // 遍历 queueOfGarbageCollectedEntries集合，清除已经被 GC 的缓存项 value
    while ((sv = (SoftEntry) queueOfGarbageCollectedEntries.poll()) != null) {
      delegate.removeObject(sv.key);
    }
  }

  private static class SoftEntry extends SoftReference<Object> {
    private final Object key;

    SoftEntry(Object key, Object value, ReferenceQueue<Object> garbageCollectionQueue) {
      // 指向 value 的引用是软引用，并且关联了 引用队列
      super(value, garbageCollectionQueue);
      // 强引用
      this.key = key;
    }
  }

}
