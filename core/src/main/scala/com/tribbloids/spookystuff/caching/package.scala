package com.tribbloids.spookystuff

import java.util
import java.util.Collections

import com.google.common.collect.MapMaker

import scala.collection.mutable

/**
  * Created by peng on 18/03/16.
  */
package object caching {

  import scala.collection.JavaConverters._

  // not concurrent, discarded
//  type MapCache[K, V] = mutable.WeakHashMap[K, V]
//  def MapCache[K, V]() = new mutable.WeakHashMap[K, V]()

  type MapCache[K, V] = scala.collection.concurrent.Map[K, V]
  def MapCache[K, V](): scala.collection.concurrent.Map[K, V] = {
    new MapMaker()
      .concurrencyLevel(4)
      .weakKeys()
      .makeMap[K, V]()
      .asScala
  }

  type ConcurrentMap[K, V] = scala.collection.concurrent.Map[K, V]
  def ConcurrentMap[K, V](): MapCache[K, V] = {
    new java.util.concurrent.ConcurrentHashMap[K, V]()
      .asScala
  }

  type ConcurrentSet[V] = mutable.Set[V]

  //TODO: change to MapAsSet? not sure if its better
  def ConcurrentSet[V](): mutable.Set[V] = {
    Collections.synchronizedSet[V](new util.HashSet[V]())
      .asScala
  }
}
