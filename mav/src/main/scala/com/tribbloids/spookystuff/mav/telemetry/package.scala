package com.tribbloids.spookystuff.mav

import org.slf4j.LoggerFactory

/**
  * Created by peng on 12/11/16.
  */
package object telemetry {

  type ProxyFactory = (Endpoint => Option[Proxy])

  implicit class ProxyFactoryView(factory: ProxyFactory) {

    /**
      * return true only of factory generates a proxy that has identical GCS outs comparing to link.proxy
      */
    def canCreate(link: Link): Boolean = {

      val dryRun = factory.apply(link.endpoint)
      val actual = link.proxyOpt
      val gcsOuts: Seq[Seq[String]] = Seq(
        dryRun,
        actual
      )
        .map {
          proxyOpt =>
            proxyOpt.map(_.outs.slice(1, Int.MaxValue))
              .getOrElse(Nil)
        }
      val result = gcsOuts.distinct.size == 1
      dryRun.foreach(_.tryClean(true))
      if (!result) {
        LoggerFactory.getLogger(this.getClass).info(
          s"""
             |Can no longer use existing telemetry link for drone ${link.endpoint.connStr}:
             |output should be routed to GCS(s) ${gcsOuts.head.mkString("[",", ","]")}
             |but instead existing one routes it to ${gcsOuts.last.mkString("[",", ","]")}
             """.stripMargin
        )
      }
      result
    }
  }
}
