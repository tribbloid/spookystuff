package com.tribbloids.spookystuff.utils

import java.util.concurrent.ArrayBlockingQueue

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.{Partition, SparkContext}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class PreemptiveLocalOps(capacity: Int)(
    implicit exeCtx: ExecutionContext = PartitionExecution.exeCtx
) {

  import SpookyViews._

  trait Impl[T] {

    def partitionExes: Stream[PartitionExecution[T]]

    lazy val partitions: Stream[Partition] = partitionExes.map(_.partition)

    lazy val sc: SparkContext = partitionExes.head.rdd.sparkContext

    def toLocalPartitionIterator: Iterator[Array[T]] = SparkHelper.withScope(sc) {

      val buffer = new ArrayBlockingQueue[Try[PartitionExecution[T]]](capacity)

      val p = SparkLocalProperties(sc)

      Future {

        sc.setJobGroup(p.groupID, p.description)

        sc.withJob(PreemptiveLocalOps.this.toString) {

          partitionExes.foreach { exe =>
            buffer.put(Success(exe)) // may be blocking due to capacity
            exe.AsArray.start // non-blocking
          }
        }
      }.onFailure {
        case e: Throwable =>
          buffer.put(Failure(e))
      }

      partitionExes.indices.toIterator.map { _ =>
        val exe = buffer.take().get
        exe.AsArray.get
      }
    }

    def toLocalIterator: Iterator[T] = {

      toLocalPartitionIterator.flatMap(v => v.iterator)
    }
  }

  case class ForRDD[T](self: RDD[T]) extends Impl[T] {

    override lazy val partitionExes: Stream[PartitionExecution[T]] = {

      self.partitions.indices.toStream.map { i =>
        PartitionExecution[T](self, i)
      }
    }
  }

  case class ForDataset[T](self: Dataset[T]) extends Impl[T] {

    lazy val delegate: ForRDD[T] = ForRDD(self.rdd)

    override def partitionExes: Stream[PartitionExecution[T]] = delegate.partitionExes
  }

}
