package com.tribbloids.spookystuff.mav.comm


import com.tribbloids.spookystuff.session.python.CaseInstanceRef

import scala.collection.mutable.ArrayBuffer

object ProxyFactory {

  val usedPorts: ArrayBuffer[Int] = ArrayBuffer.empty
}

/**
  * Created by peng on 29/10/16.
  */
// if out is empty it means an empty node, the most left traversal path of the tree is always the
case class ProxyFactory(
                         //primary localhost out port number -> list of URLs for multicast
                         //the first one used by DK, others nobody cares
                         ports: Seq[Int] = 12014 to 12108,
                        //this is the default port listened by QGCS
                         gcsMapping: Endpoint => Seq[String] = _ => Seq("udp:localhost:14550"),
                         //connection string (RegEx?) pattern => GCS URLs
                         polling: Boolean = false
                       ) {

  def next(endpoint: Endpoint): Proxy = {
    ProxyFactory.synchronized {
      val port = ports.find(v => !ProxyFactory.usedPorts.contains(v))
        .get //TODO: orElse
      val outs = gcsMapping(endpoint)
      val result = Proxy(
        endpoint.connStrs.head,
        endpoint.name,
        port,
        outs
      )
      ProxyFactory.usedPorts += port
      result
    }
  }
}

object Proxy {

  def apply(
             master: String,
             name: String,
             port: Int,
             gcsOuts: Seq[String]
           ): Proxy = {
    val outs = Seq(s"udp:localhost:$port") ++ gcsOuts
    Proxy(
      master,
      name,
      outs
    )
  }
}

case class Proxy(
                  master: String,
                  name: String,
                  outs: Seq[String]
                ) extends CaseInstanceRef