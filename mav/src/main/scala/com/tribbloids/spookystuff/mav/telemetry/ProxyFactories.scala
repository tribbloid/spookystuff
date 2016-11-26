package com.tribbloids.spookystuff.mav.telemetry

/**
  * Created by peng on 26/11/16.
  */
object ProxyFactories {

  def canCreate(factory: ProxyFactory, link: Link): Boolean = {

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
    dryRun.foreach(_.tryClean())
    result
  }

  case object NoProxy extends ProxyFactory {
    def apply(endpoint: Endpoint) = None

    //    override def unapply(link: Link): Boolean = {
    //      link.proxy.isEmpty
    //    }
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        primaryOuts: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        gcsMapping: Endpoint => Set[String] = _ => Set("udp:localhost:14550"),
                        //connection string (RegEx?) pattern => GCS URLs
                        polling: Boolean = false
                      ) extends ProxyFactory {

    def apply(endpoint: Endpoint): Option[Proxy] = {
      ForkToGCS.synchronized {
        val primaryOut = primaryOuts.find(v => !Proxy.existing.flatMap(_.outs.headOption).contains(v))
          .get //TODO: orElse
        val outs = gcsMapping(endpoint).toSeq
        val result = Proxy(
          endpoint.connStrs.head,
          Seq(primaryOut) ++ outs
        )
        Some(result)
      }
    }

    //    override def unapply(link: Link): Boolean = {
    //      val expectedGCSOuts: Set[String] = gcsMapping(link.endpoint)
    //      link.proxy match {
    //        case None => false
    //        case Some(pp) =>
    //          if (pp.outs.slice(1, Int.MaxValue).toSet == expectedGCSOuts) true
    //          else false
    //      }
    //    }
  }
}
