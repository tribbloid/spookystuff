package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.utils.Caching

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC. Always enabled
  */
object InMemoryDocCache extends AbstractDocCache {

  val internal: Caching.ConcurrentCache[Trace, Seq[Fetched]] = Caching.ConcurrentCache()

  def cacheable(v: Seq[Fetched]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.InMemory])
  }

  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]] = {
    val candidate = internal.get(k)
    candidate.flatMap { v =>
      if (
        v.exists { vv =>
          !inTimeRange(k.last, vv, spooky)
        }
      )
        None
      else
        Some(v)
    }
  }

  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {
    internal.put(k, v)
    this
  }
}
