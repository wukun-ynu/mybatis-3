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
package org.apache.ibatis.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.ibatis.reflection.ArrayUtil;

/**
 * @author Clinton Begin
 */
public class CacheKey implements Cloneable, Serializable {

  private static final long serialVersionUID = 1146682552656046210L;

  public static final CacheKey NULL_CACHE_KEY = new CacheKey() {

    private static final long serialVersionUID = 1L;

    @Override
    public void update(Object object) {
      throw new CacheException("Not allowed to update a null cache key instance.");
    }

    @Override
    public void updateAll(Object[] objects) {
      throw new CacheException("Not allowed to update a null cache key instance.");
    }
  };

  private static final int DEFAULT_MULTIPLIER = 37;
  private static final int DEFAULT_HASHCODE = 17;

  // 参与计算hashCode，默认值DEFAULT_MULTIPLYER = 37
  private final int multiplier;
  // 当前CacheKey对象的hashcode，默认值DEFAULT_HASHCODE = 17
  private int hashcode;
  // 校验和
  private long checksum;
  private int count;
  // 8/21/2017 - Sonarlint flags this as needing to be marked transient. While true if content is not serializable, this
  // is not always true and thus should not be marked transient.
  // 由该集合中的所有元素 共同决定两个CacheKey对象是否相同，一般会使用一下四个元素
  // MappedStatement的id、查询结果集的范围参数（RowBounds的offset和limit）
  // SQL语句（其中可能包含占位符"?"）、SQL语句中占位符的实际参数
  private List<Object> updateList;

  public CacheKey() {
    this.hashcode = DEFAULT_HASHCODE;
    this.multiplier = DEFAULT_MULTIPLIER;
    this.count = 0;
    this.updateList = new ArrayList<>();
  }

  public CacheKey(Object[] objects) {
    this();
    updateAll(objects);
  }

  public int getUpdateCount() {
    return updateList.size();
  }

  public void update(Object object) {
    int baseHashCode = object == null ? 1 : ArrayUtil.hashCode(object);

    // 重新计算count、checksum和hashcode的值
    count++;
    checksum += baseHashCode;
    baseHashCode *= count;

    hashcode = multiplier * hashcode + baseHashCode;

    // 将object添加到updateList集合
    updateList.add(object);
  }

  public void updateAll(Object[] objects) {
    for (Object o : objects) {
      update(o);
    }
  }

  /**
   * CacheKey重写了 equals() 和 hashCode()方法，这两个方法使用上面介绍
   * 的 count、checksum、hashcode、updateList 比较两个 CacheKey对象 是否相同
   */
  @Override
  public boolean equals(Object object) {
    // 如果为同一对象，直接返回 true
    if (this == object) {
      return true;
    }
    // 如果 object 都不是 CacheKey类型，直接返回 false
    if (!(object instanceof CacheKey)) {
      return false;
    }

    // 类型转换一下
    final CacheKey cacheKey = (CacheKey) object;

    // 依次比较 hashcode、checksum、count，如果不等，直接返回 false
    if ((hashcode != cacheKey.hashcode) || (checksum != cacheKey.checksum) || (count != cacheKey.count)) {
      return false;
    }

    // 比较 updateList 中的元素是否相同，不同直接返回 false
    for (int i = 0; i < updateList.size(); i++) {
      Object thisObject = updateList.get(i);
      Object thatObject = cacheKey.updateList.get(i);
      if (!ArrayUtil.equals(thisObject, thatObject)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public String toString() {
    StringJoiner returnValue = new StringJoiner(":");
    returnValue.add(String.valueOf(hashcode));
    returnValue.add(String.valueOf(checksum));
    updateList.stream().map(ArrayUtil::toString).forEach(returnValue::add);
    return returnValue.toString();
  }

  @Override
  public CacheKey clone() throws CloneNotSupportedException {
    CacheKey clonedCacheKey = (CacheKey) super.clone();
    clonedCacheKey.updateList = new ArrayList<>(updateList);
    return clonedCacheKey;
  }

}
