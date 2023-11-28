package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.doc.{DocUtils, Fetched}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.hadoop.fs.Path

import java.util.UUID

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC. Always enabled
  */
object DFSDocCache extends AbstractDocCache {

  def cacheable(v: Seq[Fetched]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.DFS])
  }

  def getImpl(k: TraceView, spooky: SpookyContext): Option[Seq[Fetched]] = {

    val pathStr = CommonUtils.\\\(
      spooky.dirConf.cache,
      spooky.spookyConf.cacheFilePath(k)
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

    val pathStr = CommonUtils.\\\(
      spooky.dirConf.cache,
      spooky.spookyConf.cacheFilePath(k),
      UUID.randomUUID().toString
    )

    DocUtils.cache(v, pathStr)(spooky)
    this
  }
}
