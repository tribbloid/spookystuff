package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.{PreemptiveLocalOps, SCFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuf.IncrementallyCachedRDDSuite.WithRDD
import org.apache.spark.sql.spookystuf.FastForwardingIterator
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.collection.{immutable, mutable}
import scala.util.Random

class IncrementallyCachedRDDSuite extends FunSpec with BeforeAndAfterAll {

  val size = 100000

  case class Sub(numPartitions: Int) extends FunSpec {
    override def suiteName: String = numPartitions.toString

    val groundTruth: WithRDD = WithRDD(TestSC.parallelize(1 to size, numPartitions))

    it("block cache always compute the entire partition") {

      val u = WithRDD(TestSC.parallelize(1 to size, numPartitions))
      val sliced = u.cached.getSlice(0, 2)

      val n = sliced.size

      assert(n == numPartitions * 2)
      assert(u.count.value == size)
      assert(u.localAcc.get() <= size)
    }

    describe("without fastForward") {

      it("should only cache the necessary part of the partition") {

        val u = WithRDD(TestSC.parallelize(1 to size, numPartitions))

        for (i <- 0 to 9) {

          val sliced = SCFunctions.withJob(s"slice $i") {

            u.incrementallyCached.getSlice(i, i + 2)
          }

          assert(sliced == groundTruth.src.getSlice(i, i + 2))

          assert(sliced.size == numPartitions * 2)
          assert(u.count.value == numPartitions * (2 + i))
          assert(u.localAcc.get() <= numPartitions * (2 + i))
        }
      }

      it("... even if using preemptive collection") {

        val u = WithRDD(TestSC.parallelize(1 to size, numPartitions), { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        })

        for (i <- 0 to 9) {

          val sliced = u.incrementallyCached.getSlice(i, i + 2)

          assert(sliced == groundTruth.src.getSlice(i, i + 2))

          assert(sliced.size == numPartitions * 2)
          assert(u.count.value == numPartitions * (2 + i))
          assert(u.localAcc.get() <= numPartitions * (2 + i))
        }
      }
    }

    describe("with fastForward") {

      it("should only cache the necessary part of the partition") {

        val u = WithRDD(TestSC.parallelize(1 to size, numPartitions))

        for (i <- 0 to 9) {

          val sliced = SCFunctions.withJob(s"slice $i") {

            u.incrementallyCached.getSlice_fastForward(i, i + 2)
          }

          assert(u.ffInvoked.value > 0)

          assert(sliced == groundTruth.src.getSlice(i, i + 2))

          assert(sliced.size == numPartitions * 2)
          assert(u.count.value == numPartitions * (2 + i))
          assert(u.localAcc.get() <= numPartitions * (2 + i))
        }
      }

      it("... even if the starting index exceeds the length of the cached array") {

        val u = WithRDD(TestSC.parallelize(1 to size, numPartitions))

        for (i <- 0 to 9) {

          val sliced = SCFunctions.withJob(s"slice $i") {

            u.incrementallyCached.getSlice_fastForward(4 * i, 4 * i + 2)
          }

          assert(u.ffInvoked.value > 0)

          assert(sliced == groundTruth.src.getSlice(4 * i, 4 * i + 2))

          assert(sliced.size == numPartitions * 2)
          assert(u.count.value == numPartitions * (4 * i + 2))
          assert(u.localAcc.get() <= numPartitions * (4 * i + 2))
        }
      }

      it("... even if using preemptive collection") {

        val u = WithRDD(TestSC.parallelize(1 to size, numPartitions), { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        })

        for (i <- 0 to 9) {

          val sliced = u.incrementallyCached.getSlice_fastForward(i, i + 2)

          assert(u.ffInvoked.value > 0)

          assert(sliced == groundTruth.src.getSlice(i, i + 2))

          assert(sliced.size == numPartitions * 2)
          assert(u.count.value == numPartitions * (2 + i))
          assert(u.localAcc.get() <= numPartitions * (2 + i))
        }
      }
    }
  }

  override def nestedSuites: immutable.IndexedSeq[Suite] = immutable.IndexedSeq(
    Sub(1),
    Sub(3),
    Sub(8),
    Sub(21),
    Sub(64)
  )

//  override def afterAll(): Unit = {
//
//    super.afterAll()
//
//    SCFunctions.blockUntilKill(999999)
//  }
}

object IncrementallyCachedRDDSuite {

  val localAccs = mutable.Map.empty[Long, AtomicInteger]

  case class WithRDD(
      _src: RDD[Int],
      action: RDD[Int] => List[Int] = _.collect().toList
  ) {

    val id: Long = Random.nextLong()

    def localAcc: AtomicInteger = localAccs.getOrElseUpdate(id, new AtomicInteger())

    val count = new LongAccumulator()
    TestSC.register(count, "count")

    val ffInvoked = new LongAccumulator()
    TestSC.register(ffInvoked, "ffInvoked")

    val rdd: RDD[Int] = _src.map { v =>
      localAcc.getAndIncrement()
      count add 1
//      println(v)
      v
    }

    case class Case(
        rdd: RDD[Int]
    ) {

      def outer: WithRDD = WithRDD.this

      def getSlice(from: Int, to: Int): List[Int] = action(
        rdd
          .mapPartitions { itr =>
            itr.slice(from, to)
          }
      )

      def getSlice_fastForward(from: Int, to: Int): immutable.Seq[Int] = action(
        rdd
          .mapPartitions {
            case itr: FastForwardingIterator[Int] =>
              ffInvoked.add(1)
              itr.fastForward(from).slice(0, to - from)
            case itr =>
              itr.slice(from, to)
          }
      )
    }

    lazy val src: Case = Case(_src)

    lazy val cached: Case = Case(rdd.persist())

    lazy val incrementallyCached: Case = {
      Case(
        new IncrementallyCachedRDD(rdd, 100, 100)
      )
    }
  }
}
