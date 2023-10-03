package com.tribbloids.spookystuff.utils

import org.sparkproject.guava.cache.CacheBuilder

import scala.collection.mutable

object CachingUtils {

  import scala.jdk.CollectionConverters._

  /**
    * A cache designed for multithreaded usage in cases where values (not keys) contained in this map will not
    * necessarily be cleanly removed. This map uses weak references for contained values (not keys) in order to ensure
    * that the existance of a reference to that object in this cache will not prevent the garbage collection of the
    * contained object.
    *
    * <p><b>Warning:</b> DO NOT use .weakKeys()!. Otherwise, the resulting map will use identity ({@code ==}) comparison
    * to determine equality of keys, which is a technical violation of the {@link Map} specification, and may not be
    * what you expect.
    *
    * @throws IllegalStateException
    *   if the key strength was already set
    * @see
    *   WeakReference
    */
  type ConcurrentCache[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentCache[K, V](): ConcurrentCache[K, V] = {

    // TODO: switching to https://github.com/blemale/scaffeine if faster?
    val base = CacheBuilder
      .newBuilder()
      .concurrencyLevel(CommonUtils.numLocalCores)
      .weakValues() // This ensures that being present in this map will not prevent garbage collection/finalization
      .build[Object, Object]()
      .asMap()

    val asScala = base.asScala

    asScala.asInstanceOf[ConcurrentCache[K, V]]
  }

  type ConcurrentSoftCache[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentSoftCache[K, V](): ConcurrentCache[K, V] = {

    val base = CacheBuilder
      .newBuilder()
      .concurrencyLevel(CommonUtils.numLocalCores)
      .softValues()
      .build[Object, Object]()
      .asMap()

    val asScala = base.asScala

    asScala.asInstanceOf[ConcurrentCache[K, V]]
  }

  type ConcurrentMap[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentMap[K, V](): ConcurrentMap[K, V] = {
    new java.util.concurrent.ConcurrentHashMap[K, V]().asScala
  }

  type ConcurrentSet[V] = ConcurrentMap[V, Unit]
  def ConcurrentSet[V](): ConcurrentSet[V] = {
    ConcurrentMap[V, Unit]()
  }

  implicit class MapView[K, V](self: mutable.Map[K, V]) {

    def getOrUpdateSync(key: K)(value: => V): V = {

      self.getOrElse(
        key, {
          self.synchronized {
            self.getOrElseUpdate(
              key,
              value
            )
          }
        }
      )
    }
  }
}
