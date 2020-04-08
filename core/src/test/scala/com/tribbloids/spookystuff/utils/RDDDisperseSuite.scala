package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.ml.dsl.utils.ScalaNameMixin
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

class RDDDisperseSuite extends SpookyEnvFixture with SparkJobGroupMixin with HasEager {

  implicit val concurrentCtx: ExecutionContextExecutor = ExecutionContext.global

  object Consts {

    val size: Int = Random.nextInt(90000) + 10000
    val data: Range = 1 to size

    val fixedTgtPartRange: Range = Range(1, sc.defaultParallelism, Math.sqrt(sc.defaultParallelism).toInt)

    val smallNPart: Int = Random.nextInt(10) + fixedTgtPartRange.max

    val tgtNPart: Int = Random.nextInt(100) + smallNPart * 2
    val expectedMinNNonEmptyPart: Int = 2

    val pSizeGen: Int => Long = { _ =>
      1000L
    }
  }

//  object Consts {
//
//    val size: Int = 1000
//    val data: Range = 1 to size
//
//    val fixedTgtPartRange: Range = 1 to 4
//
//    val smallNPart: Int = sc.defaultParallelism
//
//    val tgtNPart: Int = smallNPart + 24
//    val expectedMinNNonEmptyPart: Int = 2
//
//    val pSizeGen: Int => Long = { i =>
//      10L
//    }
//  }

  import Consts._

  def assertWellFormed(rdd: RDD[Int]): Unit = {

    assert(rdd.partitions.length === tgtNPart)

    val nonEmptySizes = rdd
      .mapPartitions { itr =>
        val _size = itr.toList.size
        if (_size > 0) Iterator(_size)
        else Iterator()
      }
      .collect()

    val nonEmptyCount = nonEmptySizes.length

    LoggerFactory
      .getLogger(this.getClass)
      .info(
        s"$nonEmptyCount partition(s) of ${rdd.partitions.length} are non-empty"
      )

    Predef.assert(nonEmptyCount >= expectedMinNNonEmptyPart)

    val lists: Seq[(String, List[Int])] = Seq(
      "collect" -> rdd.collect().toList,
      "toLocalIterator" -> rdd.toLocalIterator.toList,
      "toLocalIteratorPreemptively" -> PreemptiveLocalOps(CommonUtils.numLocalCores)
        .ForRDD(rdd)
        .toLocalIterator
        .toList
    )

    lists.foreach {
      case (n, ll) =>
        assert(ll.size === size, n)
        val set1 = ll.toSet
        val set2 = data.toSet

        Predef.assert(
          set1 == set2,
          n + ": " + (set1.union(set2) -- set1.intersect(set2))
        )
    }

//    val ii = rdd.toLocalIterator.toList
  }

  def assertCanBeBalanced_raw(rdd: RDD[Int]): Unit = {
    import SpookyViews._

    val balanced = rdd
      .Disperse(pSizeGen)
      .asRDD(tgtNPart)

    assertWellFormed(balanced)

    rdd.unpersist(true)
  }

  trait Facet extends Eager with ScalaNameMixin {

    val acc: LongAccumulator = sc.longAccumulator(this.facetName)
    var nPart: Int = -1

    final def assertCanBeBalanced(rdd: RDD[Int]): Unit = {

      val acc = this.acc

      acc.reset()
      Predef.assert(acc.value == 0)

      val _rdd = rdd.map { v =>
        acc.add(1)
//        val task = TaskContext.get()
//        println(
//          s"stage = ${task.stageId()}, task = ${task.taskAttemptId()}, partition = ${task.partitionId()}"
//        )
        v
      }

      doAssert(_rdd)

    }

    def doAssert(rdd: RDD[Int]): Unit

    def facetName: String = this.objectSimpleName

    describe(facetName) {

      for (ii <- fixedTgtPartRange) {

        it(s"RDD with $ii partition") {
          nPart = ii
          val rdd = sc.parallelize(data, nPart)

          assertCanBeBalanced(rdd)
        }
      }

      it("RDD with many partitions") {
        nPart = smallNPart
        val rdd = sc.parallelize(data, nPart)

        assertCanBeBalanced(rdd)
      }

      it("RDD with skewed partitions most of which are empty") {
        nPart = smallNPart
        val rdd = sc.parallelize(Seq(data), nPart).flatMap(identity)

        assertCanBeBalanced(rdd)
      }

    }
  }

  object PlainRDD extends Facet with NOTEager {

    override def doAssert(rdd: RDD[Int]): Unit = {

      assertCanBeBalanced_raw(rdd)

      assert(acc.value === size * 4 * nPart)
    }
  }

  object PartitionReified extends Facet with NOTEager {

    override def doAssert(rdd: RDD[Int]): Unit = {

      val s = rdd.mapPartitions { itr =>
        val list = itr.toList

        list.iterator
      }

      assertCanBeBalanced_raw(s)

      val expected = size * 4 * nPart * tgtNPart

      assert(
        acc.value === expected
      )
    }
  }

  class Persisted(level: StorageLevel) extends Facet {

    override def doAssert(rdd: RDD[Int]): Unit = {

      rdd.persist(level)
      assertCanBeBalanced_raw(rdd)

      assert(acc.value <= size * TestHelper.numComputers)
    }

    override def facetName: String = super.facetName + ": " + level.description
  }

  class Persisted_RDDReified(level: StorageLevel) extends Persisted(level) {

    override def doAssert(rdd: RDD[Int]): Unit = {

      val p = rdd.persist(level)
      p.foreach(_ => {})

      assertCanBeBalanced_raw(p)

      assert(acc.value === size)
    }
  }

  class Checkpointed(level: StorageLevel) extends Persisted(level) {

    TestHelper.enableCheckpoint

    override def doAssert(rdd: RDD[Int]): Unit = {

      val p = rdd.persist(level)
      p.checkpoint()
      p.foreach(_ => {})
      p.unpersist(true)

      assertCanBeBalanced_raw(p)

      assert(acc.value === size)
    }
  }

  object Persisted_PartitionReified extends Facet {

    override def doAssert(rdd: RDD[Int]): Unit = {

      val s = rdd
        .mapPartitions { itr =>
          val list = itr.toList

          list.iterator
        }
        .persist()

      assertCanBeBalanced_raw(s)

      assert(acc.value <= size * TestHelper.numComputers)
    }
  }

  object Persisted_PartitionShuffled extends Facet {

    override def doAssert(rdd: RDD[Int]): Unit = {

      val shuffleSeed = Random.nextLong()

      val s = rdd
        .mapPartitions { itr =>
//        val list = itr.toList

          val shuffled = BufferedShuffleIteratorV1(itr, seed = shuffleSeed)

          shuffled
        }
        .persist()

      assertCanBeBalanced_raw(s)

      assert(acc.value <= size * TestHelper.numComputers)
    }
  }

  {
    import StorageLevel._

    reifyEager()

    Seq(MEMORY_ONLY, MEMORY_ONLY_SER_2, MEMORY_AND_DISK).flatMap { level =>
      Seq(
        new Persisted(level),
        new Persisted_RDDReified(level),
        new Checkpointed(level)
      )
    }
  }

  it("Benchmark: can be much faster than ") {

    //TODO: move to benchmark and implement!
  }
}
