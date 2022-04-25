package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentCache

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC.
  * Always enabled
  */
object InMemoryDocCache extends AbstractDocCache {

  val internal: ConcurrentCache[Trace, Seq[DocOption]] = ConcurrentCache()

  def cacheable(v: Seq[DocOption]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.InMemory])
  }

  def getImpl(k: TraceView, spooky: SpookyContext): Option[Seq[DocOption]] = {
    val candidate = internal.get(k)
    candidate.flatMap { v =>
      if (v.exists { vv =>
            !inTimeRange(k.last, vv, spooky)
          })
        None
      else
        Some(v)
    }
  }

  def putImpl(k: Trace, v: Seq[DocOption], spooky: SpookyContext): this.type = {
    internal.put(k, v)
    this
  }
}
