package com.tribbloids.spookystuff.utils

import org.apache.spark.sql._SQLHelper
import java.util.concurrent.Executors

import org.apache.spark.{FutureAction, Partition}
import org.apache.spark.rdd.RDD

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.reflect.ClassTag

case class PartitionExecution[T](
    @transient rdd: RDD[T],
    partitionID: Int, // TODO: should be plural?
    jobTextOvrd: Option[String] = None
) {

  implicit val classTag: ClassTag[T] = _SQLHelper.rddClassTag(rdd)

  lazy val partition: Partition = rdd.partitions(partitionID)

  override lazy val toString: String = {
    s"$partitionID - RDD ${rdd.id}"
  }

  def onStart(): Unit = {}

  def onCompletion(): Unit = {}

  case object AsArray {

    @transient lazy val future: FutureAction[Array[T]] = {

      onStart()

      var result: Array[T] = null

      val future = rdd.context.submitJob[T, Array[T], Array[T]](
        rdd,
        _.toArray,
        Seq(partitionID),
        { (_, data) =>
          result = data
        },
        result
      )

      future
    }

    def start: PartitionExecution.this.type = {
      future
      PartitionExecution.this
    }

    def get: Array[T] = {

      val result = future.get()

      onCompletion()
      result
    }
  }
}

object PartitionExecution {

  val exeCtx: ExecutionContextExecutor = {

    val threadPool = Executors.newCachedThreadPool()
    val executionContext = ExecutionContext.fromExecutor(threadPool)
    executionContext
  }
}
