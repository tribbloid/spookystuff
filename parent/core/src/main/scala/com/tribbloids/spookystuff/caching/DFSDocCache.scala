package com.tribbloids.spookystuff.caching

import ai.acyclic.prover.commons.Envs.Dir
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.{DocUtils, Observation}
import org.apache.hadoop.fs.Path

import java.util.UUID

/**
  * Backed by a WeakHashMap, the web cache temporarily store all trace -> Array[Page] until next GC. Always enabled
  */
object DFSDocCache extends AbstractDocCache {

  def cacheable(v: Seq[Observation]): Boolean = {
    v.exists(v => v.cacheLevel.isInstanceOf[DocCacheLevel.DFS])
  }

  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Observation]] = {

    val pathStr =
      Dir(spooky.dirConf.cache) :\
        spooky.conf.cacheFilePaths(k)

    val (earliestTime: Long, latestTime: Long) = getTimeRange(k.last, spooky)

    val pages = DocUtils.restoreLatest(
      new Path(pathStr),
      earliestTime,
      latestTime
    )(spooky)

    Option(pages)
  }

  def putImpl(k: Trace, v: Seq[Observation], spooky: SpookyContext): this.type = {

    val pathStr =
      Dir(spooky.dirConf.cache) :\
        spooky.conf.cacheFilePaths(k) :\
        UUID.randomUUID().toString

    DocUtils.cache(v, pathStr)(spooky)
    this
  }
}
