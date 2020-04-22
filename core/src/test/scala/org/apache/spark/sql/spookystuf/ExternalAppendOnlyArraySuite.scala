package org.apache.spark.sql.spookystuf

import org.apache.spark.rdd.UnionRDD
import org.apache.spark.util.LongAccumulator
import org.scalatest.{FunSpec, Ignore}

// TODO: why this test doesn't work?
@Ignore
class ExternalAppendOnlyArraySuite extends FunSpec {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  val parallelism: Int = TestSC.defaultParallelism
//  val parallelism: Int = 1

  describe("cachedOrComputeIterator") {

    it("instances in multiple threads can share cache") {

      val rdd = TestSC.parallelize(0 to 1, 1).map { v =>
        new ExternalAppendOnlyArray[Int](100, 100)
      }

      rdd.persist()

      val computed = new LongAccumulator()
      TestSC.register(computed)

      val children = (1 to parallelism).map { i =>
        rdd.mapPartitions { itr =>
          /**
            * the purpose of this line is to avoid no thread other than the first one to use its iterator and fast-forward
            * 1: -> -> -> ->
            * 2: C  C  C
            * 3: C  C
            */
          Thread.sleep(i * 1000)
          val array = itr.next()

          val computeItr = (1 to 1000).iterator.map { v =>
            computed.add(1)
            v
          }

          array.Impl().cachedOrComputeIterator(computeItr)
        }
      }

      val combined = new UnionRDD(TestSC, children)

      val result = combined.collect().toSeq

      assert(result.size == parallelism * 1000)
      assert(result == Seq(1 to parallelism).flatMap(i => 1 to 1000))
      assert(computed.value == 1000)
    }
  }
}
