package com.tribbloids.spookystuff.utils

import java.util.concurrent.Executors

import org.apache.spark.{FutureAction, Partition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.utils.SparkHelper

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.reflect.ClassTag

case class PartitionExecution[T](
    @transient rdd: RDD[T],
    partitionID: Int // TODO: should be plural?
) {

  implicit val classTag: ClassTag[T] = SparkHelper.rddClassTag(rdd)

  lazy val partition: Partition = rdd.partitions(partitionID)

  case object AsArray {

    def start: PartitionExecution.this.type = {
      AsArray.future
      PartitionExecution.this
    }

    @transient lazy val future: FutureAction[Array[T]] = {

      var result: Array[T] = null

      val future = rdd.context.submitJob[T, Array[T], Array[T]](
        rdd,
        _.toArray,
        Seq(partitionID), { (_, data) =>
          result = data
        },
        result
      )

      future
    }

    @transient lazy val get: Array[T] = future.get()
  }
}

object PartitionExecution {

  val exeCtx: ExecutionContextExecutor = {

    val threadPool = Executors.newCachedThreadPool()
    val executionContext = ExecutionContext.fromExecutor(threadPool)
    executionContext
  }
}
