package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.mav.telemetry.{Endpoint, Link, LinkWithContext}
import com.tribbloids.spookystuff.utils.PrettyProduct
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object LinkFactories {

  /**
    * return true only of factory generates a proxy that has identical GCS outs comparing to link.proxy
    */
  def canCreate(factory: LinkFactory, link: LinkWithContext): Boolean = {

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

  case object NoProxy extends LinkFactory with PrettyProduct{
    def apply(endpoint: Endpoint) = Link(endpoint, Nil)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        uriCandidates: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        gcsMapping: Endpoint => Set[String] = _ => Set("udp:localhost:14550")
                      ) extends LinkFactory with PrettyProduct {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: Endpoint): Link = {

      LinkFactories.synchronized {
        val existingURIs = Link.existing.values.toSeq.map(_.link.uri)
        val availableURIs = uriCandidates.filter {
          v =>
            !existingURIs.contains(v)
        }

        val primaryOut = availableURIs.apply(Random.nextInt(availableURIs.size))
        val outs = gcsMapping(endpoint).toSeq
        val result = Link(
          endpoint,
          Seq(primaryOut) ++ outs
        )
        result
      }
    }
  }
}
