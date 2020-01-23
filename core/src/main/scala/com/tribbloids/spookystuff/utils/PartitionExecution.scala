package com.tribbloids.spookystuff.utils

import java.util.concurrent.Executors

import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.reflect.ClassTag

case class PartitionExecution[T: ClassTag](
    @transient self: RDD[T],
    partitionID: Int
) {

  def eager: this.type = {
    AsArray.future
    this
  }

  case object AsArray {

    @transient lazy val future: FutureAction[Array[T]] = {

      var result: Array[T] = null

      val future = self.context.submitJob[T, Array[T], Array[T]](
        self,
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
