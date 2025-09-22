package com.tribbloids.spookystuff.caching

import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.doc.Observation

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC. Always enabled
  */
object InMemoryDocCache extends AbstractDocCache {

  val internal: Caching.ConcurrentCache[CacheKey, Seq[Observation]] = Caching.ConcurrentCache()

  def isCacheable(v: Seq[Observation]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.InMemory])
  }

  def getImpl(key: CacheKey, spooky: SpookyContext): Option[Seq[Observation]] = {
    val candidate = internal.get(key)
    candidate.flatMap { v =>
      if (
        v.exists { vv =>
          !inTimeRange(key.lastAction, vv, spooky)
        }
      )
        None
      else
        Some(v)
    }
  }

  def putImpl(key: CacheKey, v: Seq[Observation], spooky: SpookyContext): this.type = {
    internal.put(key, v)
    this
  }
}
