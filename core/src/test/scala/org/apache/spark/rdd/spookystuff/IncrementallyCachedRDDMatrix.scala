package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.dsl.Samplers
import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.PreemptiveLocalOps
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuff.IncrementallyCachedRDDSuite.TestSubject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import scala.collection.immutable
import scala.languageFeature.existentials

class IncrementallyCachedRDDMatrix extends FunSpec with BeforeAndAfterAll {

  import com.tribbloids.spookystuff.testutils.TestHelper.TestSQL.implicits._

  val nPartitions = Seq(
    Samplers.withReplacement(1 to TestSC.defaultParallelism).get,
    Samplers.withReplacement(TestSC.defaultParallelism to 64).get
  )

  override def nestedSuites: immutable.IndexedSeq[IncrementallyCachedRDDSuite[_]] = {

    val result = nPartitions.flatMap { nPartition =>
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
          intRDD, { rdd =>
            PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
          }
        )

        def onInternalRows: TestSubject[InternalRow] = {

          TestSubject(
            rowRDD, { rdd =>
              rdd
                .map(v => v.getInt(0))
                .collect()
                .toList
            }
          )
        }

        //TODO: this doesn't work at the moment, fix and enable
        def onPersisted = TestSubject(persistedSrc)

        def onCheckpointed = TestSubject(checkpointedSrc)
      }

      object Collect {

        case object Ints extends IncrementallyCachedRDDSuite[Int](nPartition) {

          override def getFacet = WithRDDs.useCollect.incCached
        }

        case object InternalRows extends IncrementallyCachedRDDSuite[InternalRow](nPartition) {

          override def getFacet = WithRDDs.onInternalRows.incCached
        }

        case object Unsafe extends IncrementallyCachedRDDSuite[InternalRow](nPartition) {

          override def getFacet = WithRDDs.onInternalRows.incCached_unsafe
        }

        case object Checkpointed extends IncrementallyCachedRDDSuite[Int](nPartition) {

          override def getFacet = WithRDDs.onCheckpointed.incCached
        }
      }

      object PreemptiveGet {

        case object Ints extends IncrementallyCachedRDDSuite[Int](nPartition) {

          override def getFacet = WithRDDs.useToLocalItr.incCached
        }
      }

      Seq(
        Collect.Ints,
        Collect.InternalRows,
        Collect.Unsafe,
        Collect.Checkpointed,
        PreemptiveGet.Ints
      )
    }

    result.to[immutable.IndexedSeq]
  }

//  override def afterAll(): Unit = {
//
//    super.afterAll()
//
//    SCFunctions.blockUntilKill(999999)
//  }
}
