package com.tribbloids.spookystuff.mav

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
      val gcsOuts: Seq[Option[Seq[String]]] = Seq(
        dryRun,
        actual
      )
        .map {
          proxyOpt =>
            proxyOpt.map(_.outs.slice(1, Int.MaxValue))
        }
      val result = gcsOuts.distinct.size == 1
      dryRun.foreach(_.tryClean(true))
      result
    }
  }
}
