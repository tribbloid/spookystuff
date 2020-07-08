package com.tribbloids.spookystuff.utils

import java.io.EOFException
import java.util.concurrent.ArrayBlockingQueue

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.utils.SparkHelper
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object PreemptiveLocalOps {

  object EOFMark extends EOFException()
}

case class PreemptiveLocalOps(capacity: Int)(
    implicit exeCtx: ExecutionContext = PartitionExecution.exeCtx
) {

  import PreemptiveLocalOps._
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

      val activeSpark = SparkSession.active

      Future {

        SparkSession.setActiveSession(activeSpark)
        sc.setJobGroup(p.groupID, p.description)

        wIndex.foreach {
          case (partitionExecution, ii) =>
            val exe = partitionExecution

            val jobText = exe.jobTextOvrd.getOrElse(
              s"$ii\t/ $numPartitionsStr (preemptive)"
            )

            LoggerFactory.getLogger(this.getClass).info(s"Scheduling [$exe] $jobText")

            sc.withJob(jobText) {
              exe.AsArray.start // non-blocking
              buffer.put( // may be blocking due to capacity
                Success(exe))
            }
        }

        buffer.put(
          Failure(EOFMark)
        )

      }.onFailure {
        case e: Throwable =>
          buffer.put(Failure(e))
      }

      val result = Iterator
        .from(0)
        .map { _ =>
          buffer.take()
        }
        .takeWhile {
          case Failure(EOFMark) => false
          case _                => true
        }
        .map { trial =>
          val exe = trial.get

          LoggerFactory.getLogger(this.getClass).info(s"Collecting: [$exe]")
          val array = exe.AsArray.get
          LoggerFactory.getLogger(this.getClass).info(s"Job done  : [$exe]")
          array
        }

      result
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
