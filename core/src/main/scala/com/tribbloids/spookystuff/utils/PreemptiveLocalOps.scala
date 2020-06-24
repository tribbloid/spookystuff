package com.tribbloids.spookystuff.utils

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.utils.SparkHelper

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class PreemptiveLocalOps(capacity: Int)(
    implicit exeCtx: ExecutionContext = PartitionExecution.exeCtx
) {

  import SpookyViews._

  trait Impl[T] {

    def partitionIterator: Iterator[PartitionExecution[T]]

    lazy val wIndex: Iterator[(PartitionExecution[T], Int)] = partitionIterator.zipWithIndex

    def numPartitionsOpt: Option[Int]

    lazy val numPartitionsStr: String = numPartitionsOpt.map("" + _).getOrElse("(Unknown)")

    def sc: SparkContext

    def toLocalPartitionIterator: Iterator[Array[T]] = SparkHelper.withScope(sc) {

      val buffer = new ArrayBlockingQueue[Try[PartitionExecution[T]]](capacity)

      val p = SparkLocalProperties(sc)

      @volatile var isFinished = false
      val traversed = new AtomicInteger(0)

      Future {

        sc.setJobGroup(p.groupID, p.description)

        wIndex.foreach {
          case (factory, ii) =>
            traversed.incrementAndGet()

            val exe = factory

            val jobText = exe.jobTextOvrd.getOrElse(
              s"$ii\t/ $numPartitionsStr (preemptive)"
            )

            sc.withJob(jobText) {
              buffer.put( // may be blocking due to capacity
                Success(exe.AsArray.start) // non-blocking
              )
            }
        }
        isFinished = true

      }.onFailure {
        case e: Throwable =>
          buffer.put(Failure(e))
      }

      Iterator
        .from(0)
        .takeWhile { i =>
          (!isFinished) || (i < traversed.get())
        }
        .map { _ =>
          val exe = buffer.take().get
          exe.AsArray.get
        }
    }

    def toLocalIterator: Iterator[T] = {

      toLocalPartitionIterator.flatMap(v => v.iterator)
    }
  }

  case class ForRDD[T](self: RDD[T]) extends Impl[T] {

    def sc: SparkContext = self.sparkContext

    override lazy val partitionIterator: Iterator[PartitionExecution[T]] = {

      self.partitions.iterator.map(_.index).map { i =>
        PartitionExecution[T](self, i)
      }
    }

    override def numPartitionsOpt: Option[Int] = Some(self.partitions.length)
  }

  case class ForDataset[T](self: Dataset[T]) extends Impl[T] {

    def sc: SparkContext = self.sparkSession.sparkContext

    lazy val delegate: ForRDD[T] = ForRDD(self.rdd) // TODO: should use internal RDD for smaller serialization size

    override def partitionIterator: Iterator[PartitionExecution[T]] = delegate.partitionIterator

    override def numPartitionsOpt: Option[Int] = delegate.numPartitionsOpt
  }
}
