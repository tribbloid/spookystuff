package com.tribbloids.spookystuff.utils

import org.sparkproject.guava.cache.CacheBuilder

import scala.collection.mutable
import scala.language.implicitConversions

trait Caching {

  import scala.jdk.CollectionConverters._

  trait CacheTag

  def guavaBuilder: CacheBuilder[AnyRef, AnyRef]

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
  type ConcurrentCache[K, V] = scala.collection.concurrent.Map[K, V] with CacheTag

  def ConcurrentCache[K, V](): ConcurrentCache[K, V] = {

    // TODO: switching to https://github.com/blemale/scaffeine if faster?
    val base = guavaBuilder
      .asInstanceOf[CacheBuilder[K, V]]
      .build[K, V]()
      .asMap()

    val asScala = base.asScala

    asScala.asInstanceOf[ConcurrentCache[K, V]]
  }
}

object Caching {

  import scala.jdk.CollectionConverters._

  trait ConcurrentTag

  def javaConcurrentMap[K, V]() = new java.util.concurrent.ConcurrentHashMap[K, V]()

  type ConcurrentMap[K, V] = scala.collection.concurrent.Map[K, V] with ConcurrentTag
  def ConcurrentMap[K, V](): ConcurrentMap[K, V] = {
    javaConcurrentMap[K, V]().asScala.asInstanceOf[ConcurrentMap[K, V]]
  }

  type ConcurrentSet[V] = mutable.Set[V] with ConcurrentTag
  def ConcurrentSet[V](): ConcurrentSet[V] = {

    val proto: mutable.Set[V] = javaConcurrentMap[V, Unit]().keySet(()).asScala

    //    val proto: mutable.Set[V] = javaConcurrentMap[V, Unit]().keySet().asScala// don't use, Java doesn't have a default value for Unit

    proto.asInstanceOf[ConcurrentSet[V]]
  }

  object Strong extends Caching {

    override lazy val guavaBuilder: CacheBuilder[AnyRef, AnyRef] = {
      CacheBuilder
        .newBuilder()
        .concurrencyLevel(CommonUtils.numLocalCores)
    }
  }

  object Weak extends Caching {

    override lazy val guavaBuilder: CacheBuilder[AnyRef, AnyRef] = {
      CacheBuilder
        .newBuilder()
        .concurrencyLevel(CommonUtils.numLocalCores)
        .weakValues() // This ensures that being present in this map will not prevent garbage collection/finalization
    }
  }

  object Soft extends Caching {

    override lazy val guavaBuilder: CacheBuilder[AnyRef, AnyRef] = {
      CacheBuilder
        .newBuilder()
        .concurrencyLevel(CommonUtils.numLocalCores)
        .softValues() // values are only GC'd when closing to max heap
    }
  }

  type ConcurrentCache[K, V] = Soft.ConcurrentCache[K, V]
  implicit def defaultImpl(v: this.type): Soft.type = Soft

}
