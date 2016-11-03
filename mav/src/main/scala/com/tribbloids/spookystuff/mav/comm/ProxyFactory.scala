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
                         gcsMapping: Endpoint => Seq[String] = _ => Nil,
                         //connection string (RegEx?) pattern => GCS URLs
                         polling: Boolean = false
                       ) {

  def next(endpoint: Endpoint): Proxy = {
    ProxyFactory.synchronized {
      val unusedPort = ports.find(v => !ProxyFactory.usedPorts.contains(v))
        .get //TODO: orElse
      val outs = gcsMapping(endpoint)
      val result = Proxy(
        endpoint.connStrs.head,
        endpoint.name,
        unusedPort,
        outs
      )
      result
    }
  }
}

case class Proxy(
                  connStr: String,
                  name: String,
                  port: Int,
                  outs: Seq[String]
                ) extends CaseInstanceRef