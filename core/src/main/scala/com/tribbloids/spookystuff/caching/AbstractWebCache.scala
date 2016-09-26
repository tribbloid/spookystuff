package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.{SpookyContext, dsl}
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.extractors.{FR, Literal}

/**
  * Created by peng on 07/06/16.
  */
trait AbstractWebCache {

  import dsl._

  def get(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]] = {
    val pagesOpt = getImpl(k, spooky)

    val dryrun = k.dryrun

    pagesOpt.foreach {
      pages =>
        for (page <- pages) {
          val pageBacktrace: Trace = page.uid.backtrace
          val similarDryrun = dryrun.find(_ == pageBacktrace).get

          pageBacktrace.injectFrom(similarDryrun) //this is to allow actions in backtrace to have different name than those cached
        }
    }
    pagesOpt
  }
  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]]

  def put(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {

    if (v.exists(!_.cacheable)) this
    else putImpl(k, v, spooky)
  }
  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type

  def putIfAbsent(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type = {

    if (v.exists(!_.cacheable)) {}
    else if (getImpl(k, spooky).nonEmpty) {}
    else {
      put(k, v, spooky)
    }

    this
  }

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