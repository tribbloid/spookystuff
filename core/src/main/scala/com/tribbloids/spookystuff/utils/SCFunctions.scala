package com.tribbloids.spookystuff.utils

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

class SCFunctions(sc: SparkContext) {

  def scLocalProperties: SparkLocalProperties = SparkLocalProperties(sc)

  import SpookyViews._

  // Test is rare, how to make sure that it is working
  def withJob[T](
      description: String = null,
      groupID: String = null,
      descriptionBreadcrumb: Boolean = true,
      delimiter: String = " \u2023 "
  )(fn: => T): T = {

    val old = scLocalProperties

    val _descriptionParts =
      if (descriptionBreadcrumb)
        Seq(old.description, description).flatten(Option(_))
      else
        Option(description).toSeq

    val _description =
      if (_descriptionParts.isEmpty) null
      else _descriptionParts.mkString(delimiter)

    val _groupID = Seq(old.groupID, groupID).flatten(Option(_)).lastOption.orNull

    sc.setJobGroup(_groupID, _description)

    val result = try {
      fn
    } finally {

      sc.setJobGroup(old.groupID, old.description)
    }

    result
  }

  /**
    * similar to .parallelize(), except that order of each datum is preserved,
    * and locality is preserved even if the RDD is computed many times.
    * Also, the RDD is distributed to executor cores as evenly as possible.
    * If parallesism > self.defaultParallelism then theoretically it is
    * guaranteed to have at least 1 datum on each executor thread, however
    * this is not tested thoroughly in large scale, and may be nullified by Spark optimization.
    */
  def seed[T: ClassTag](
      seq: Seq[T],
      parallelismOpt: Option[Int] = None,
      mustHaveNonEmptyPartitions: Boolean = false
  ): RDD[(Int, T)] = {
    val size = parallelismOpt.getOrElse(sc.defaultParallelism)
    val kvs = seq.zipWithIndex.map(_.swap)
    val raw: RDD[(Int, T)] = sc.parallelize(kvs, size)
    val sorted = raw.sortByKey(ascending = true, numPartitions = size)
    //        .partitionBy(new HashPartitioner(self.defaultParallelism)) //TODO: should use RangePartitioner?
    //        .persist()
    //      seed.count()
    //      val seed = self.makeRDD[Int]((1 to self.defaultParallelism).map(i => i -> Seq(i.toString)))

    assert(
      sorted.partitions.length == size,
      s"seed doesn't have the right number of partitions: expected $size, actual ${sorted.partitions.length}"
    )

    val result =
      if (!mustHaveNonEmptyPartitions) sorted
      else {
        sorted.mapPartitions { itr =>
          assert(itr.hasNext)
          itr
        }
      }
    result
  }

  //    def bareSeed(
  //                  parallelismOpt: Option[Int] = None
  //                ): RDD[(Int, Unit)] = {
  //
  //      val n = parallelismOpt.getOrElse(self.defaultParallelism)
  //      val uuids = (1 to n).map(_ => Unit)
  //      seed(uuids, parallelismOpt, mustHaveNonEmptyPartitions = true)
  //    }

  def uuidSeed(
      parallelismOpt: Option[Int] = None,
      debuggingInfo: Option[String] = None
  ): RDD[(Int, UUID)] = {

    val n = parallelismOpt.getOrElse(sc.defaultParallelism)
    val uuids: immutable.Seq[UUID] = (1 to n).map(_ => UUID.randomUUID())
    debuggingInfo.foreach { info =>
      LoggerFactory
        .getLogger(this.getClass)
        .info(
          s"""
             |$info
             |${uuids.mkString("\n")}
             """.stripMargin
        )
    }

    seed(uuids, parallelismOpt, mustHaveNonEmptyPartitions = true)
  }

  //TODO: this should be superseded by https://github.com/apache/spark/pull/22192
  def runEverywhere[T: ClassTag](alsoOnDriver: Boolean = true)(f: ((Int, UUID)) => T): Seq[T] = {
    val localFuture: Option[Future[T]] =
      if (alsoOnDriver) Some(Future[T] {
        f(-1 -> UUID.randomUUID())
      }(ExecutionContext.global))
      else {
        None
      }

    val n = sc.defaultParallelism * SpookyViewsSingleton.REPLICATING_FACTOR
    val onExecutors = uuidSeed(Some(n))
      .mapOncePerWorker { f }
      .collect()
      .toSeq

    localFuture.map { future =>
      Await.result(future, Duration.Inf)
    }.toSeq ++ onExecutors
  }

  def allTaskLocationStrs: Seq[String] = {
    runEverywhere(alsoOnDriver = false) { _ =>
      SpookyUtils.taskLocationStrOpt.get
    }
  }

  def blockUntilKill(timeMillis: Long): Try[Array[(Int, Int)]] = {

    withJob(s"⛔ Blocking for ${timeMillis}ms! This job can be killed to proceed ⛔") {

      Try {
        seed(Seq(0), Some(1))
          .map { v =>
            Thread.sleep(timeMillis)
            v
          }
          .collect()
      }
    }

  }

  //TODO: remove! not useful
  //    def allExecutorCoreIDs = {
  //      mapAtLeastOncePerExecutorCore {
  //        val thread = Thread.currentThread()
  //        (SpookyUtils.blockManagerIDOpt, thread.getId, thread.getName)
  //      }
  //        .collect()
  //    }
}

object SCFunctions extends SCFunctions(SparkContext.getOrCreate())
