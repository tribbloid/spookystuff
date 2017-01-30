package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.{SpookyContext, dsl}
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.extractors.{FR, Literal}

/**
  * Created by peng on 07/06/16.
  */
trait AbstractDocCache {

  import dsl._

  def get(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]] = {
    val pagesOpt = getImpl(k, spooky)

    val dryrun = k.dryrun

    val result = pagesOpt.map {
      pages =>
        for (page <- pages) yield {
          val pageBacktrace: Trace = page.uid.backtrace
          val similarTrace = dryrun.find(_ == pageBacktrace).get

          pageBacktrace.injectFrom(similarTrace) //this is to allow actions in backtrace to have different name than those cached
          page.update(
            uid = page.uid.copy()(name = Option(page.uid.output).map(_.name).orNull)
          )
        }
    }
    result
  }
  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]]

  def getOrElsePut(k: Trace, v: Seq[Fetched], spooky: SpookyContext): Seq[Fetched] = {

    val gg = get(k, spooky)
    gg.getOrElse {
      put(k, v, spooky)
      v
    }
  }

  def cacheable(v: Seq[Fetched]): Boolean

  def put(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {

    if (cacheable(v)) putImpl(k, v, spooky)
    else this
  }
  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type

  def inTimeRange(action: Action, fetched: Fetched, spooky: SpookyContext): Boolean = {
    val range = getTimeRange(action, spooky)

    (range._1 < fetched.timeMillis) && (fetched.timeMillis < range._2)
  }

  def getTimeRange(action: Action, spooky: SpookyContext): (Long, Long) = {
    val waybackOption = action match {
      case w: Wayback =>
        Option(w.wayback).map {
          expr =>
            val result = expr.asInstanceOf[Literal[FR, Long]].value
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
      case None => System.currentTimeMillis()
    }

    val earliestTime = spooky.conf.getEarliestDocCreationTime(nowMillis)
    val latestTime = waybackOption.getOrElse(Long.MaxValue)
    (earliestTime, latestTime)
  }
}