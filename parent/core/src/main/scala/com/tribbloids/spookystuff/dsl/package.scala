package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.dsl.LocalityLike.Instance
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup, SpookySchema}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}

import java.io.File
import java.util.UUID
import scala.reflect.ClassTag

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

  type GenLocality = LocalityLike[LocalityGroup, Any]
  type Locality = LocalityLike[LocalityGroup, LocalityGroup]

  object Locality {

    case object Passthrogh extends LocalityLike.PassThrough

    // this won't merge identical traces and do lookup, only used in case each resolve may yield different result
    case object Narrow extends GenLocality {

      def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
        Inst[K]()
      }

      case class Inst[K]()(
          implicit
          val ctg: ClassTag[K]
      ) extends Instance[K] {

        override def reduceByKey[V: ClassTag](
            rdd: RDD[(K, V)],
            reducer: (V, V) => V,
            beaconRDDOpt: Option[BeaconRDD[K]] = None
        ): RDD[(K, V)] = {
          rdd.mapPartitions { itr =>
            itr.toTraversable
              .groupBy(_._1) // TODO: is it memory efficient? Write a test case for it
              .map(v => v._1 -> v._2.map(_._2).reduce(reducer))
              .iterator
          }
        }
      }
    }

    case class Wide(
        partitionerFactory: RDD[?] => Partitioner = {
          PartitionerFactory.SamePartitioner
        }
    ) extends GenLocality {

      def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
        Inst[K]()
      }

      case class Inst[K]()(
          implicit
          val ctg: ClassTag[K]
      ) extends Instance[K] {

        // this is faster and saves more memory
        override def reduceByKey[V: ClassTag](
            rdd: RDD[(K, V)],
            reducer: (V, V) => V,
            beaconRDDOpt: Option[BeaconRDD[K]] = None
        ): RDD[(K, V)] = {
          val partitioner = partitionerFactory(rdd)
          rdd.reduceByKey(partitioner, reducer)
        }
      }
    }

    // group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
    // reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
    case class DocCacheAware(
        partitionerFactory: RDD[?] => Partitioner = {
          PartitionerFactory.SamePartitioner
        }
    ) extends GenLocality {

      def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
        Inst[K](schema.ec)
      }

      case class Inst[K](
          ec: ExecutionContext
      )(
          implicit
          val ctg: ClassTag[K]
      ) extends Instance[K] {

        override def _createBeaconRDD(
            ref: RDD[?]
        ): Option[BeaconRDD[K]] = {

          val partitioner = partitionerFactory(ref)
          val result = ref.sparkContext
            .emptyRDD[(K, Unit)]
            .partitionBy(partitioner)
          ec.tempRefs.persist(result, StorageLevel.MEMORY_AND_DISK)
          result.count()
          Some(result)
        }

        override def reduceByKey[V: ClassTag](
            rdd: RDD[(K, V)],
            reducer: (V, V) => V,
            beaconRDDOpt: Option[BeaconRDD[K]] = None
        ): RDD[(K, V)] = {

          val beaconRDD = beaconRDDOpt.get

          val partitioner = partitionerFactory(rdd)
          val cogrouped = rdd
            .cogroup(beaconRDD, beaconRDD.partitioner.getOrElse(partitioner))
          cogrouped.mapValues { tuple =>
            tuple._1.reduce(reducer)
          }
        }
      }
    }

    // case object AutoDetect extends QueryOptimizer
  }

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
