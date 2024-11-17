package org.apache.spark.rdd.spookystuff

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import org.apache.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuff.NarrowDispersedRDD.*

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag
import scala.util.Random

object NarrowDispersedRDD {

  class RoundRobinDependencyImpl[T](prev: RDD[T]) extends NarrowDependency[T](prev) {

    lazy val prevNPartitions: Int = prev.partitions.length

    override def getParents(partitionId: Int): Seq[Int] = {
      // get parents partition IDs on the same machine

      Seq(partitionId % prevNPartitions)
    }
  }

  case class NarrowMapping(
      prev: Int,
      private val _to: Seq[Int],
      seed: Long = Random.nextLong()
  ) {

    @transient lazy val to: Seq[Int] = _to.sorted

    @transient lazy val narrowIndexLookup: Map[Int, Int] = Map(to.zipWithIndex*)

    @transient lazy val maxNarrowIndex: Int = narrowIndexLookup.values.max
  }

  trait PseudoRandom extends Serializable {

    def Generator: NarrowMapping => PRGen

    case class Execution[T](
        mapping: NarrowMapping,
        prevPartition: Iterator[T],
        narrowIndex: Int
    ) extends NOTSerializable {

      val gen: PRGen = Generator(mapping)

      def computePartition: Iterator[T] = {
        prevPartition
          .takeWhile { v =>
            gen.hasMore(v, narrowIndex)
          }
          .flatMap { v =>
            val tgt = gen.nextNarrowIndex(v)
            val result = if (tgt == narrowIndex) {
              Some(v)
            } else {
              None
            }
            result
          }
      }
    }
  }

  trait PRGen extends NOTSerializable {

    def nextNarrowIndex(datum: Any): Int // narrowIndex -> is the last

    def hasMore(datum: Any, narrowIndex: Int): Boolean
  }

  case class ByRange(
      pSizeGen: Int => Long
  ) extends PseudoRandom {

    case class Generator(
        mapping: NarrowMapping
    ) extends PRGen {

      val pSizes: Seq[Long] = mapping.to.map(pSizeGen)

      val counter: AtomicInteger = new AtomicInteger(0)
      val state_narrowIndex: AtomicInteger = new AtomicInteger(0)

      override def nextNarrowIndex(datum: Any): Int = {
        val state_size = pSizes(state_narrowIndex.get())
        val result = state_narrowIndex

        if (counter.getAndIncrement() >= state_size) {
          state_narrowIndex.incrementAndGet()
          if (state_narrowIndex.get() > mapping.maxNarrowIndex)
            state_narrowIndex.set(mapping.maxNarrowIndex)

          counter.set(0)
        }

        result.get()
      }

      override def hasMore(datum: Any, narrowIndex: Int): Boolean = {
        state_narrowIndex.get() <= narrowIndex
      }
    }
  }

  case class PartitionImpl(
      override val index: Int,
      @transient prev: RDD[?],
      pseudoRandom: PseudoRandom,
      mappings: Seq[NarrowMapping],
      preferredLocationOvrd: Option[String] = None
  ) extends Partition {

    val prevID: Int = prev.id
    val prevIndices: Seq[Int] = mappings.map(_.prev)
    val prevPartitions: Seq[Partition] = {

      prevIndices.map { k =>
        prev.partitions(k)
      }
    }

//    @transient lazy val prGens: Seq[PRGen] =
//      mappings.map(v => pseudoRandom.Generator(v, v.seed))

    def getPreferredLocations(partition: Partition): Seq[String] = {
      preferredLocationOvrd.map(Seq(_)).getOrElse {
        prevIndices.flatMap { prevIndex =>
          val locs = prev.sparkContext
            .getPreferredLocs(prev, prevIndex)

          locs.map(_.host)
        }
      }
    }
  }
}

class NarrowDispersedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    numPartitions: Int,
    pseudoRandom: PseudoRandom
) extends RDD[T](
      prev.context,
      Nil // useless, already have overridden getDependencies
    ) {

  import NarrowDispersedRDD.*

  val seed: Long = Random.nextLong()

  override def getDependencies: Seq[RoundRobinDependencyImpl[T]] = {
    Seq(new RoundRobinDependencyImpl(prev))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  override def getPartitions: Array[Partition] = {

    val deps = getDependencies

    val idPairs: Seq[(Int, Int)] = (0 until numPartitions).flatMap { i =>
      val parents = deps.flatMap { dep =>
        dep.getParents(i)
      }

      parents.map { parent =>
        parent -> i
      }
    }

    val idMap = idPairs
      .groupBy(_._1)
      .mapValues { vs =>
        vs.map(_._2)
      }

    val mappings: Seq[NarrowMapping] = idMap.map {
      case (k, v) =>
        NarrowMapping(k, v)
    }.toList

    val result = for (i <- 0 until numPartitions) yield {

      val relevantMappings: Seq[NarrowMapping] = mappings.filter(_.to.contains(i))

      PartitionImpl(
        i,
        prev,
        pseudoRandom,
        relevantMappings
      )
    }
    result.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val p: PartitionImpl = partition.asInstanceOf[PartitionImpl]
    val prevRDD: RDD[T] = firstParent[T]

    SparkEnv.get.blockManager

    val toPartitions = p.prevPartitions.zip(p.mappings).map {
      case (prevPartition, mapping) =>
//        val prevBlockId = RDDBlockId(prevRDD.id, prevPartition.index)
//
//        val cacheResult = blockMgr.getOrElseUpdate(
//          prevBlockId,
//          StorageLevel.MEMORY_ONLY, // not sure if it may blow up
//          elementClassTag, { () =>
//            prevRDD.getOrCompute(prevPartition, context)
//          }
//        )

        val itr: Iterator[T] = prevRDD.iterator(prevPartition, context)

        val exe = pseudoRandom.Execution(mapping, itr, mapping.narrowIndexLookup(p.index))

        val result = exe.computePartition

//        blockMgr.remove

        result
    }

    toPartitions.iterator.flatten
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[PartitionImpl].getPreferredLocations(partition)
  }
}
