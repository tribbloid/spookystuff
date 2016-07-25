package com.tribbloids.spookystuff.caching

import java.util.UUID

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.{DocUtils, Fetched}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.hadoop.fs.Path

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC.
  * Always enabled
  */
object DFSWebCache extends AbstractWebCache {

  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]] = {

    val pathStr = SpookyUtils.pathConcat(
      spooky.conf.dirs.cache,
      spooky.conf.cacheFilePath(k).toString
    )

    val (earliestTime: Long, latestTime: Long) = getTimeRange(k.last, spooky)

    val pages = DocUtils.restoreLatest(
      new Path(pathStr),
      earliestTime,
      latestTime
    )(spooky)

    Option(pages)
  }

  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {

    val pathStr = SpookyUtils.pathConcat(
      spooky.conf.dirs.cache,
      spooky.conf.cacheFilePath(v.head.uid.backtrace).toString,
      UUID.randomUUID().toString
    )

    DocUtils.cache(v, pathStr)(spooky)
    this
  }
}