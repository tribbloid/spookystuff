package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuf.IncrementallyCachedRDDSuite.Upstream
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.collection.immutable

class IncrementallyCachedRDDSuite extends FunSpec with BeforeAndAfterAll {

  case class Sub(numPartitions: Int) extends FunSpec {

    it("block cache always compute the entire partition") {

      val u = Upstream(TestSC.parallelize(1 to 100, numPartitions))

      u.rdd.persist()

      val sliced = u.rdd
        .mapPartitions { itr =>
          itr.slice(0, 2)
        }

      val n = sliced.count()

      assert(n == numPartitions * 2)
      assert(u.acc.value == 100)
      assert(u.localAcc.get() <= 100)
    }

    it("incremental cache only cache the necessary part of the partition") {

      val u = Upstream(TestSC.parallelize(1 to 100, numPartitions))

//        val rdd2 = rdd
      val rdd2 = new IncrementallyCachedRDD(u.rdd, 1000, 1000)

      val sliced = rdd2.mapPartitions { itr =>
        itr.slice(0, 2)
      }
      val n = sliced.count()

      assert(n == numPartitions * 2)
      assert(u.acc.value == numPartitions * 2)
      assert(u.localAcc.get() <= numPartitions * 2)

      val sliced2 = rdd2.mapPartitions { itr =>
        itr.slice(1, 3)
      }
      val n2 = sliced2.count()
      assert(n2 == numPartitions * 2)
      assert(u.acc.value == numPartitions * 3)
      assert(u.localAcc.get() <= numPartitions * 3)
    }

  }

  override def nestedSuites: immutable.IndexedSeq[Suite] = immutable.IndexedSeq(
    Sub(1),
    Sub(16)
  )

//  override def afterAll(): Unit = {
//
//    super.afterAll()
//
//    SCFunctions.blockUntilKill(999999)
//  }
}

object IncrementallyCachedRDDSuite {

  case class Upstream(src: RDD[Int]) {

    val localAcc: AtomicInteger = new AtomicInteger()

    val acc = new LongAccumulator()
    TestSC.register(acc)

    val rdd: RDD[Int] = src.map { v =>
      localAcc.getAndIncrement()
      acc add 1
//      println("!!!!!")
      v
    }
  }
}
