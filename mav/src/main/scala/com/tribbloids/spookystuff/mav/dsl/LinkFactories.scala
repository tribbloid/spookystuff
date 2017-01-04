package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.mav.telemetry.{Drone, Link, LinkWithContext}
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

    val dryRun = factory.apply(link.link.drone)
    dryRun.isDryrun = true
    val actual = link.link
    val links = Seq(
      dryRun,
      actual
    )
    dryRun.clean()
    val gcsOutss: Seq[Set[String]] = links
      .map {
        link =>
          link.gcsOuts.toSet
      }

    val result = gcsOutss.distinct.size == 1
    if (!result) {
      LoggerFactory.getLogger(this.getClass).info (
        s"""
           |Can no longer use existing telemetry link for drone ${link.link.nativeEndpoint.connStr}:
           |output should be routed to GCS(s) ${gcsOutss.head.mkString("[",", ","]")}
           |but instead existing one routes it to ${gcsOutss.last.mkString("[",", ","]")}
             """.trim.stripMargin
      )
    }
    result
  }

  case object NoProxy extends LinkFactory with PrettyProduct{
    def apply(endpoint: Drone) = Link(endpoint)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        getExecutorOuts: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        getGCSOuts: Drone => Set[String] = _ => Set("udp:localhost:14550"),
                        executorOutsSize: Int = 1
                      ) extends LinkFactory with PrettyProduct {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: Drone): Link = {

      LinkFactories.synchronized {
        val existing4Exec: Seq[String] = Link.existing.values.toSeq
          .flatMap(_.link.allURI)
        val available4Exec = getExecutorOuts.filter {
          v =>
            !existing4Exec.contains(v)
        }
        val shuffled = Random.shuffle(available4Exec)

        val executorOuts = shuffled.slice(0, executorOutsSize)
        val gcsOuts = getGCSOuts(endpoint).toSeq
        val result = Link(
          endpoint,
          executorOuts,
          gcsOuts
        )
        result
      }
    }
  }
}
