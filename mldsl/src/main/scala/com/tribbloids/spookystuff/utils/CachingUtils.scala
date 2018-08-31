package com.tribbloids.spookystuff.utils

import com.google.common.cache.CacheBuilder

import scala.collection.mutable

object CachingUtils {

  import scala.collection.JavaConverters._

  // not concurrent, discarded
  //  type MapCache[K, V] = mutable.WeakHashMap[K, V]
  //  def MapCache[K, V]() = new mutable.WeakHashMap[K, V]()

  // TODO: switching to https://github.com/blemale/scaffeine if faster?
  type ConcurrentCache[K, V] = scala.collection.concurrent.Map[K, V]

  /**
    * <p><b>Warning:</b> DO NOT use .weakKeys()!. Otherwise, the resulting map will use identity ({@code ==})
    * comparison to determine equality of keys, which is a technical violation of the {@link Map}
    * specification, and may not be what you expect.
    *
    * @throws IllegalStateException if the key strength was already set
    * @see WeakReference
    */
  def ConcurrentCache[K, V](): ConcurrentCache[K, V] = {
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(CommonUtils.numDriverCores)
      .softValues()
      .build[Object, Object]()
      .asMap()
      .asScala
      .asInstanceOf[ConcurrentCache[K, V]]
  }

  type ConcurrentMap[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentMap[K, V](): ConcurrentMap[K, V] = {
    new java.util.concurrent.ConcurrentHashMap[K, V]().asScala
  }

  type ConcurrentSet[V] = mutable.Set[V]

  def ConcurrentSet[V](): mutable.Set[V] = {
    //    new ConcurrentHashMap[V, Unit]().keySet().asScala //TODO: switch to this
    new mutable.HashSet[V]() with mutable.SynchronizedSet[V]
  }
}
