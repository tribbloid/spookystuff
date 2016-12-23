package com.tribbloids.spookystuff

import com.google.common.cache.CacheBuilder
import com.tribbloids.spookystuff.utils.SpookyUtils

import scala.collection.mutable

/**
  * Created by peng on 18/03/16.
  */
package object caching {

  import scala.collection.JavaConverters._

  // not concurrent, discarded
  //  type MapCache[K, V] = mutable.WeakHashMap[K, V]
  //  def MapCache[K, V]() = new mutable.WeakHashMap[K, V]()

  type ConcurrentCache[K, V] = scala.collection.concurrent.Map[K, V]

  /**
    * <p><b>Warning:</b> DO NOT use .weakKeys()!. Otherwise, the resulting map will use identity ({@code ==})
    * comparison to determine equality of keys, which is a technical violation of the {@link Map}
    * specification, and may not be what you expect.
    *
    * @throws IllegalStateException if the key strength was already set
    * @see WeakReference
    */
  def ConcurrentCache[K <: AnyRef, V <: AnyRef](): ConcurrentCache[K, V] = {
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(SpookyUtils.numCores)
      .softValues()
      .build[K, V]()
      .asMap()
      .asScala
  }

  type ConcurrentMap[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentMap[K, V](): ConcurrentMap[K, V] = {
    new java.util.concurrent.ConcurrentHashMap[K, V]()
      .asScala
  }

  type ConcurrentSet[V] = mutable.Set[V]

  //TODO: change to MapAsSet? not sure if its better
  def ConcurrentSet[V](): mutable.SynchronizedSet[V] = {
//    Collections.synchronizedSet[V](new util.HashSet[V]())
//      .asScala
    new mutable.HashSet[V]() with mutable.SynchronizedSet[V]
  }
}
