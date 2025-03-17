package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.doc.Observation

import java.util.Date

/**
  * Created by peng on 07/06/16.
  */
trait AbstractDocCache {

  def get(trace: HasTrace, spooky: SpookyContext): Option[Seq[Observation]] = {
    val cacheKey = trace.cacheKey
    val cachedOpt = getImpl(cacheKey, spooky)

    val dryRun: Seq[Trace] = trace.dryRun

    val result = cachedOpt.map { pages =>
      for (page <- pages) yield {
        val cachedBacktrace: Trace = page.uid.backtrace
        val desiredBacktrace: Trace = dryRun.find(v => v.cacheKey.contains(cachedBacktrace)).get

        val export = desiredBacktrace.last.asInstanceOf[Export] // TODO: too punishable in runtime

//        Trace(cachedBacktrace)
//          .injectFrom(
//            Trace(desiredBacktrace)
//          ) // this is to allow actions in backtrace to have different name than those cached
        page.updated(
          uid = page.uid.copy(
            backtrace = desiredBacktrace,
            `export` = export
          )(
            name = export.name
          )
        )
      }
    }
    result
  }
  def getImpl(key: CacheKey, spooky: SpookyContext): Option[Seq[Observation]]

//  def getOrElsePut(k: Trace, v: Seq[Observation], spooky: SpookyContext): Seq[Observation] = {
//
//    val gg = get(k, spooky)
//    gg.getOrElse {
//      put(k, v, spooky)
//      v
//    }
//  }

  def isCacheable(v: Seq[Observation]): Boolean

  def put(trace: HasTrace, v: Seq[Observation], spooky: SpookyContext): this.type = {

    if (isCacheable(v)) {

      val cacheKey = trace.cacheKey
      putImpl(cacheKey, v, spooky)
    } else
      this
  }
  def putImpl(key: CacheKey, v: Seq[Observation], spooky: SpookyContext): this.type

  def inTimeRange(action: Action, fetched: Observation, spooky: SpookyContext): Boolean = {
    val range = getTimeRange(action, spooky)

    (range._1 < fetched.timeMillis) && (fetched.timeMillis < range._2)
  }

  def getTimeRange(action: Action, spooky: SpookyContext): (Long, Long) = {
    val waybackOption = action match {
      case w: CanWayback =>
        w.wayback.map { timeMillis =>
          spooky.conf.IgnoreCachedDocsBefore match {
            case Some(date) =>
              assert(
                timeMillis > date.getTime,
                s"SpookyConf.IgnoreCachedDocsBefore date (${date}) cannot be set to later than wayback date (${new Date(timeMillis)})"
              )
            case None =>
          }
          timeMillis
        }
      case _ =>
        None
    }

    val nowMillis = waybackOption match {
      case Some(wayback) => wayback
      case None          => System.currentTimeMillis()
    }

    val earliestTime = spooky.conf.getEarliestDocCreationTime(nowMillis)
    val latestTime = waybackOption.getOrElse(Long.MaxValue)
    (earliestTime, latestTime)
  }
}
