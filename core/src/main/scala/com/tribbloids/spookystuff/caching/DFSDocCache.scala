package com.tribbloids.spookystuff.caching

import java.util.UUID

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.conf.DirConf
import com.tribbloids.spookystuff.doc.{DocOption, DocUtils}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.hadoop.fs.Path

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC.
  * Always enabled
  */
object DFSDocCache extends AbstractDocCache {

  DirConf // register with [[Submodules]].builderRegistry

  def cacheable(v: Seq[DocOption]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.DFS])
  }

  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[DocOption]] = {

    val pathStr = CommonUtils.\\\(
      spooky.dirConf.cache,
      spooky.spookyConf.cacheFilePath(k).toString
    )

    val (earliestTime: Long, latestTime: Long) = getTimeRange(k.last, spooky)

    val pages = DocUtils.restoreLatest(
      new Path(pathStr),
      earliestTime,
      latestTime
    )(spooky)

    Option(pages)
  }

  def putImpl(k: Trace, v: Seq[DocOption], spooky: SpookyContext): this.type = {

    val pathStr = CommonUtils.\\\(
      spooky.dirConf.cache,
      spooky.spookyConf.cacheFilePath(k).toString,
      UUID.randomUUID().toString
    )

    DocUtils.cache(v, pathStr)(spooky)
    this
  }
}
