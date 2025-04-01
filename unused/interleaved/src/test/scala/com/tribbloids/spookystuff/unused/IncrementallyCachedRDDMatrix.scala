package com.tribbloids.spookystuff.unused

import ai.acyclic.prover.commons.spark.TestHelper.TestSC
import com.tribbloids.spookystuff.dsl.Samplers
import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import com.tribbloids.spookystuff.utils.PreemptiveLocalOps
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuff.IncrementallyCachedRDDSuite
import org.apache.spark.rdd.spookystuff.IncrementallyCachedRDDSuite.TestSubject
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.StorageLevel
import org.scalatest.BeforeAndAfterAll

object IncrementallyCachedRDDMatrix {

  val sql: SQLContext = TestHelper.TestSQL
  import sql.implicits._

  class Group(nPartition: Int) {

    lazy val intRDD: RDD[Int] = TestSC.parallelize(1 to IncrementallyCachedRDDSuite.datasetSize, nPartition)

    lazy val rowRDD: RDD[InternalRow] = intRDD.toDF().queryExecution.toRdd

    lazy val persistedSrc: RDD[Int] = {
      intRDD.map(identity).persist(StorageLevel.DISK_ONLY)
    }

    lazy val checkpointedSrc: RDD[Int] = {
      val result = intRDD.map(identity)
      result.checkpoint()
      result
    }

    object WithRDDs {

      def useCollect: TestSubject[Int] = TestSubject(intRDD)

      def useToLocalItr: TestSubject[Int] = TestSubject(
        intRDD,
        { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        }
      )

      def onInternalRows: TestSubject[InternalRow] = {

        TestSubject(
          rowRDD,
          { rdd =>
            rdd
              .map(v => v.getInt(0))
              .collect()
              .toList
          }
        )
      }

      // TODO: this doesn't work at the moment, fix and enable
      def onPersisted: TestSubject[Int] = TestSubject(persistedSrc)

      def onCheckpointed: TestSubject[Int] = TestSubject(checkpointedSrc)
    }

    object Collect {

      case object Ints extends IncrementallyCachedRDDSuite[Int](nPartition) {

        override def getFacet: TestSubject[Int]#Fast = WithRDDs.useCollect.incCached
      }

      case object InternalRows extends IncrementallyCachedRDDSuite[InternalRow](nPartition) {

        override def getFacet: TestSubject[InternalRow]#Fast = WithRDDs.onInternalRows.incCached
      }

      case object Unsafe extends IncrementallyCachedRDDSuite[InternalRow](nPartition) {

        override def getFacet: TestSubject[InternalRow]#Fast = WithRDDs.onInternalRows.incCached_unsafe
      }

      case object Checkpointed extends IncrementallyCachedRDDSuite[Int](nPartition) {

        override def getFacet: TestSubject[Int]#Fast = WithRDDs.onCheckpointed.incCached
      }
    }

    object PreemptiveGet {

      case object Ints extends IncrementallyCachedRDDSuite[Int](nPartition) {

        override def getFacet: TestSubject[Int]#Fast = WithRDDs.useToLocalItr.incCached
      }
    }
  }

  object GLess extends Group(Samplers.withReplacement(1 to TestSC.defaultParallelism).get)

  object GMore extends Group(Samplers.withReplacement((TestSC.defaultParallelism + 1) to 64).get)
}

class IncrementallyCachedRDDMatrix extends BaseSpec with BeforeAndAfterAll {

  import IncrementallyCachedRDDMatrix._

  override lazy val nestedSuites: Vector[IncrementallyCachedRDDSuite[_]] = {

    val result = Seq(GLess, GMore).flatMap { g =>
      Seq(
        g.Collect.Ints,
        g.Collect.InternalRows,
        g.Collect.Unsafe,
        g.Collect.Checkpointed,
        g.PreemptiveGet.Ints
      )
    }

    result.to(Vector)
  }
}
