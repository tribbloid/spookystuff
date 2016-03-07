package com.tribbloids.spookystuff

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

/**
  * Created by peng on 18/03/16.
  */
package object caching {

  type MapCache[K, V] = mutable.WeakHashMap[K, V]

  type ConcurrentMap[K, V] = ConcurrentHashMap[K, V]

  def ConcurrentMap[K, V]() = new ConcurrentHashMap[K, V]()

  type ConcurrentSet[V] = util.Set[V]

  def ConcurrentSet[V](): util.Set[V] = Collections.synchronizedSet[V](new util.HashSet[V]())
}
