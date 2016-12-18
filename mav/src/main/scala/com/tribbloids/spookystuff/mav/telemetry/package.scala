package com.tribbloids.spookystuff.mav

import org.slf4j.LoggerFactory

/**
  * Created by peng on 12/11/16.
  */
package object telemetry {

  type LinkFactory = (Endpoint => Link)

  //disabled, too complex
  implicit class LinkFactoryView(factory: LinkFactory) {

    /**
      * return true only of factory generates a proxy that has identical GCS outs comparing to link.proxy
      */
    def canCreate(link: LinkWithContext): Boolean = {

      val dryRun = factory.apply(link.link.endpoint)
      dryRun.isDryrun = true
      val actual = link.link
      val links = Seq(
        dryRun,
        actual
      )
      dryRun.clean()
      val gcsOuts = links
        .map {
          link =>
            link.outs.slice(1, Int.MaxValue)
        }

      val result = gcsOuts.distinct.size == 1
      if (!result) {
        LoggerFactory.getLogger(this.getClass).info (
          s"""
             |Can no longer use existing telemetry link for drone ${link.link.endpoint.connStr}:
             |output should be routed to GCS(s) ${gcsOuts.head.mkString("[",", ","]")}
             |but instead existing one routes it to ${gcsOuts.last.mkString("[",", ","]")}
             """.trim.stripMargin
        )
      }
      result
    }
  }
}
