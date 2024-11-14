package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Wayback.WaybackLike
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.doc.Observation

/**
  * Created by peng on 07/06/16.
  */
trait AbstractDocCache {

  def get(k: Trace, spooky: SpookyContext): Option[Seq[Observation]] = {
    val pagesOpt = getImpl(k, spooky)

    val dryRun = k.dryRun

    val result = pagesOpt.map { pages =>
      for (page <- pages) yield {
        val pageBacktrace: Trace = page.uid.backtrace
        val similarTrace = dryRun.find(_ == pageBacktrace).get

        Trace(pageBacktrace)
          .injectFrom(
            Trace(similarTrace)
          ) // this is to allow actions in backtrace to have different name than those cached
        page.updated(
          uid = page.uid.copy()(name = Option(page.uid.output).map(_.name).orNull)
        )
      }
    }
    result
  }
  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Observation]]

  def getOrElsePut(k: Trace, v: Seq[Observation], spooky: SpookyContext): Seq[Observation] = {

    val gg = get(k, spooky)
    gg.getOrElse {
      put(k, v, spooky)
      v
    }
  }

  def cacheable(v: Seq[Observation]): Boolean

  def put(k: HasTrace, v: Seq[Observation], spooky: SpookyContext): this.type = {

    if (cacheable(v)) putImpl(k.asTrace, v, spooky)
    else this
  }
  def putImpl(k: Trace, v: Seq[Observation], spooky: SpookyContext): this.type

  def inTimeRange(action: Action, fetched: Observation, spooky: SpookyContext): Boolean = {
    val range = getTimeRange(action, spooky)

    (range._1 < fetched.timeMillis) && (fetched.timeMillis < range._2)
  }

  def getTimeRange(action: Action, spooky: SpookyContext): (Long, Long) = {
    val waybackOption = action match {
      case w: WaybackLike =>
        w.wayback.map { result =>
          spooky.conf.IgnoreCachedDocsBefore match {
            case Some(date) =>
              assert(result > date.getTime, "SpookyConf.pageNotExpiredSince cannot be set to later than wayback date")
            case None =>
          }
          result
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
