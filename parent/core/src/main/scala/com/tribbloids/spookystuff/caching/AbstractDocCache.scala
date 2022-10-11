package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.extractors.FR
import com.tribbloids.spookystuff.extractors.impl.Lit

/**
  * Created by peng on 07/06/16.
  */
trait AbstractDocCache {

  def get(k: TraceView, spooky: SpookyContext): Option[Seq[DocOption]] = {
    val pagesOpt = getImpl(k, spooky)

    val dryRun = k.dryRun

    val result = pagesOpt.map { pages =>
      for (page <- pages) yield {
        val pageBacktrace: Trace = page.uid.backtrace
        val similarTrace = dryRun.find(_ == pageBacktrace).get

        TraceView(pageBacktrace)
          .injectFrom(
            TraceView(similarTrace)
          ) // this is to allow actions in backtrace to have different name than those cached
        page.updated(
          uid = page.uid.copy()(name = Option(page.uid.output).map(_.name).orNull)
        )
      }
    }
    result
  }
  def getImpl(k: TraceView, spooky: SpookyContext): Option[Seq[DocOption]]

  def getOrElsePut(k: TraceView, v: Seq[DocOption], spooky: SpookyContext): Seq[DocOption] = {

    val gg = get(k, spooky)
    gg.getOrElse {
      put(k, v, spooky)
      v
    }
  }

  def cacheable(v: Seq[DocOption]): Boolean

  def put(k: Trace, v: Seq[DocOption], spooky: SpookyContext): this.type = {

    if (cacheable(v)) putImpl(k, v, spooky)
    else this
  }
  def putImpl(k: Trace, v: Seq[DocOption], spooky: SpookyContext): this.type

  def inTimeRange(action: Action, fetched: DocOption, spooky: SpookyContext): Boolean = {
    val range = getTimeRange(action, spooky)

    (range._1 < fetched.timeMillis) && (fetched.timeMillis < range._2)
  }

  def getTimeRange(action: Action, spooky: SpookyContext): (Long, Long) = {
    val waybackOption = action match {
      case w: WaybackLike =>
        Option(w.wayback).map { expr =>
          val result = expr.asInstanceOf[Lit[FR, Long]].value
          spooky.spookyConf.IgnoreCachedDocsBefore match {
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

    val earliestTime = spooky.spookyConf.getEarliestDocCreationTime(nowMillis)
    val latestTime = waybackOption.getOrElse(Long.MaxValue)
    (earliestTime, latestTime)
  }
}
