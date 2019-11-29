package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.ml.dsl.utils.ScalaNameMixin
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

class RDDDisperseSuite extends SpookyEnvFixture.DescribeJobs with HasEager {

  implicit val concurrentCtx: ExecutionContextExecutor = ExecutionContext.global

  object Consts {

    val size: Int = Random.nextInt(90000) + 10000
    val data: Range = 1 to size

    val fixedTgtPartRange: Range = 1 to sc.defaultParallelism

    val smallNPart: Int = Random.nextInt(10) + fixedTgtPartRange.max

    val tgtNPart: Int = Random.nextInt(100) + smallNPart
    val expectedMinNNonEmptyPart: Int = 2

    val pSizeGen: Int => Long = { i =>
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

    println(
      s"$nonEmptyCount partition(s) of ${rdd.partitions.length} are non-empty"
    )

    Predef.assert(nonEmptyCount >= expectedMinNNonEmptyPart)

    (1 to 3).foreach { _ =>
      val cc = rdd.collect()
      assert(cc.length === size)
      Predef.assert(cc.toSet == data.toSet)
    }

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
      val _rdd = rdd.map { v =>
        acc.add(1)
//        val task = TaskContext.get()
//        println(
//          s"stage = ${task.stageId()}, task = ${task.taskAttemptId()}, partition = ${task.partitionId()}"
//        )
        v
      }

      doAssert(_rdd)

      acc.reset()
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

      assert(acc.value === size)
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

      assert(acc.value === size)
    }
  }

  object Persisted_PartitionShuffled extends Facet {

    override def doAssert(rdd: RDD[Int]): Unit = {

      val s = rdd
        .mapPartitions { itr =>
//        val list = itr.toList

          val shuffled = BufferedShuffleIteratorV1(itr)

          shuffled
        }
        .persist()

      assertCanBeBalanced_raw(s)

      assert(acc.value === size)
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

    //    Thread.sleep(100000000)
  }
}
