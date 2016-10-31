package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.actions.CaseInstanceRef

object ProxyFactory {

}

/**
  * Created by peng on 29/10/16.
  */
// if out is empty it means an empty node, the most left traversal path of the tree is always the
case class ProxyFactory(
                         //primary localhost out port number -> list of URLs for multicast
                         //the first one used by DK, others nobody cares
                         ports: Seq[Int] = 12014 to 12108,
                         gcsMapping: Map[String, Seq[String]] = Map(),
                         //connection string (RegEx?) pattern => GCS URLs
                         polling: Boolean = false
                       )

case class Proxy(
                  connStr: String,
                  name: String,
                  port: Int,
                  outs: Seq[String]
                ) extends CaseInstanceRef