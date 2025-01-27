package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.LocalityGroup
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import java.io.File
import java.util.UUID

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type TracePath = TracePath.build._Lemma
  object TracePath {
    val build = :=>.at[Trace].to[String]

    case object Flat extends build._Impl {

      override def apply(trace: Trace): String = {

        val actionStrs = trace.map(v => v.pathText_\)

        val actionConcat = if (actionStrs.size > 4) {
          val oneTwoThree = actionStrs.slice(0, 3)
          val last = actionStrs.last
          val omitted = "..." + (trace.length - 4) + "more" + "..."

          oneTwoThree.mkString("~") + omitted + last
        } else actionStrs.mkString("~")

        val hash = "" + trace.hashCode

        SpookyUtils.canonizeFileName(actionConcat + "~" + hash)
      }
    }

    case object Hierarchical extends build._Impl {

      override def apply(trace: Trace): String = {

        val actionStrs = trace.map(v => v.pathText_\)

        val actionConcat = if (actionStrs.size > 4) {
          val oneTwoThree = actionStrs.slice(0, 3)
          val last = actionStrs.last
          val omitted = File.separator + (trace.length - 4) + "more" + File.separator

          PathMagnet.URIPath(oneTwoThree*) :/ omitted :/ last

        } else {
          PathMagnet.URIPath(actionStrs*)
        }

        val hash = "" + trace.hashCode

        SpookyUtils.canonizeUrn(actionConcat :/ hash)
      }
    }
  }

  type DocPath = DocPath.build._Lemma
  object DocPath {
    val build: :=>.BuildDomains[Doc, String] = :=>.at[Doc].to[String]

    case class UUIDName(encoder: TracePath) extends build._Impl {

      override def apply(page: Doc): String = {
        val encoded: PathMagnet.URIPath = encoder(page.uid.backtrace)
        encoded :/ UUID.randomUUID().toString
      }
    }

    case class TimeStampName(encoder: TracePath) extends build._Impl {
      override def apply(page: Doc): String = {
        val encoded: PathMagnet.URIPath = encoder(page.uid.backtrace)
        encoded :/ SpookyUtils.canonizeFileName(page.timeMillis.toString)
      }
    }
  }

  type WebProxyFactory = WebProxyFactory.build._Lemma
  object WebProxyFactory {
    val build: :=>.BuildDomains[Unit, WebProxySetting] = :=>.at[Unit].to[WebProxySetting]
    // TODO: should yield a OptionMagnet

    val NoProxy = build { _ =>
      null
    }

    val Tor = build { _ =>
      WebProxySetting("127.0.0.1", 9050, "socks5")
    }

  }

  type GenPartitioner = GenPartitionerLike[LocalityGroup, LocalityGroup]
  type AnyGenPartitioner = GenPartitionerLike[LocalityGroup, Any]

  type PartitionerFactory = PartitionerFactory.build._Lemma
  object PartitionerFactory {

    val build: :=>.BuildDomains[RDD[?], Partitioner] = :=>.at[RDD[?]].to[Partitioner]

    def PerCore(n: Int): build._Impl = build { rdd =>
      new HashPartitioner(rdd.sparkContext.defaultParallelism * n)
    }

    val SameParallelism: build._Impl = build { rdd =>
      new HashPartitioner(rdd.partitions.length)

    }

    val SamePartitioner: build._Impl = build { rdd =>
      rdd.partitioner.getOrElse {
        new HashPartitioner(rdd.partitions.length)
      }
    }

  }

}
