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

import java.util.Deque;
import java.util.LinkedList;

import org.apache.ibatis.cache.Cache;

/**
 * FIFO (first in, first out) cache decorator.
 *
 * @author Clinton Begin
 */
public class FifoCache implements Cache {

  // 被装饰者对象
  private final Cache delegate;
  // 用一个FIFO的队列记录key的顺序，其具体实现为LinkedList
  private final Deque<Object> keyList;
  // 决定了缓存的容量上限
  private int size;

  // 国际惯例，通过构造方法初始化自己的属性，缓存容量上限默认为 1024个
  public FifoCache(Cache delegate) {
    this.delegate = delegate;
    this.keyList = new LinkedList<>();
    this.size = 1024;
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public void putObject(Object key, Object value) {
    // 清除缓存项之前，先在keyList中注册
    cycleKeyList(key);
    // 存储缓存项
    delegate.putObject(key, value);
  }

  @Override
  public Object getObject(Object key) {
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    keyList.remove(key);
    return delegate.removeObject(key);
  }

  // 除了清理缓存项，还要清理key的注册列表
  @Override
  public void clear() {
    delegate.clear();
    keyList.clear();
  }

  private void cycleKeyList(Object key) {
    // 在keyList队列中注册要添加的key
    keyList.addLast(key);
    if (keyList.size() > size) {
      // 如果注册这个 key会超出容积上限，则把最老的一个缓存项清除掉
      Object oldestKey = keyList.removeFirst();
      delegate.removeObject(oldestKey);
    }
  }

}
