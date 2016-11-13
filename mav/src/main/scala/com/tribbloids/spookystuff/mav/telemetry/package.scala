package com.tribbloids.spookystuff.mav

/**
  * Created by peng on 12/11/16.
  */
package object telemetry {

  type ProxyFactory = Endpoint => Option[Proxy]

  object ProxyFactories {

    case object NoProxy extends ProxyFactory {
      def apply(endpoint: Endpoint) = None
    }

    case class Default(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        ports: Seq[Int] = 12014 to 12108,
                        //this is the default port listened by QGCS
                        gcsMapping: Endpoint => Seq[String] = _ => Seq("udp:localhost:14550"),
                        //connection string (RegEx?) pattern => GCS URLs
                        polling: Boolean = false
                      ) extends ProxyFactory {

      def apply(endpoint: Endpoint): Option[Proxy] = {
        Default.synchronized {
          val port = ports.find(v => !Proxy.usedPorts.contains(v))
            .get //TODO: orElse
          val outs = gcsMapping(endpoint)
          val result = Proxy(
            endpoint.connStrs.head,
            port,
            outs
          )
          Some(result)
        }
      }
    }
  }
}
