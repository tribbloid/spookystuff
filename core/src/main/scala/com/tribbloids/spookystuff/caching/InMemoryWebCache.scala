package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.Fetched

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC.
  */
object InMemoryWebCache extends AbstractWebCache {

  val internal: MapCache[Trace, Seq[Fetched]] = MapCache()

  def isObsolete(fetched: Fetched, spooky: SpookyContext): Boolean = {
    val earliestTime = spooky.conf.getEarliestDocCreationTime()

    fetched.timeMillis > earliestTime
  }

  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]] = {
    val candidate = internal.get(k)
    candidate.flatMap{
      v =>
        v.foreach {
          vv =>
            if (isObsolete(vv, spooky)) return None
        }
        Some(v)
    }
  }

  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {
    internal.put(k, v)
    this
  }

  def putIfAbsentImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {
    internal.putIfAbsent(k, v)
    this
  }
}