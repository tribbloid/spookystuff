package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.utils.CachingUtils
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.spookystuf.ExternalAppendOnlyArray
import org.apache.spark.{Partition, TaskContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class IncrementallyCachedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    numRowsInMemoryBufferThreshold: Int,
    numRowsSpillThreshold: Int
//    storageLevel: StorageLevel
) extends RDD[T](prev) {

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  case class Dependency(
      p: Partition
  ) extends NOTSerializable {

    lazy val cache = new ExternalAppendOnlyArray[T](numRowsInMemoryBufferThreshold, numRowsSpillThreshold)

    @volatile var _activeTask: WTask = _

    case class WTask(task: TaskContext) {

      @volatile var _commissionedBy: TaskContext = task

      def recommission(newTask: TaskContext): Unit = {

        semaphore.acquire()
        newTask.addTaskCompletionListener[Unit] { _: TaskContext =>
          semaphore.release()
        }

        if (!_commissionedBy.eq(newTask)) {

          val metrics = task.taskMetrics()
          val activeAccs = metrics.externalAccums

          for (acc <- activeAccs) {

            newTask.registerAccumulator(acc)
            acc.reset()
          }

          LoggerFactory
            .getLogger(this.getClass)
            .info(s"recommissioning ${task.taskAttemptId()}: ${_commissionedBy
              .taskAttemptId()} -> ${newTask.taskAttemptId()}, accumulators ${activeAccs.map("#" + _.id).mkString(",")}")

          _commissionedBy = newTask
        }
      }

      val semaphore = new Semaphore(1) // cannot be shared by >1 threads

      val counter = new AtomicInteger(0)

      val compute: Iterator[T] = firstParent[T].compute(p, task).map { v =>
        counter.getAndIncrement()
        v
      }

      def getOrCompute(start: Int): Iterator[T] = {

        cache.Impl(start).cachedOrComputeIterator(compute, counter.get())
      }

      def active: WTask = Option(_activeTask).getOrElse {
        regenerate()
        this
      }

      def regenerate(): Unit = {

        _activeTask = this
      }

      def output(start: Int): Iterator[T] = {

        active.recommission(this.task)
        active.getOrCompute(start)
      }
    }
  }

  case class Existing() {

    @transient lazy val self: CachingUtils.ConcurrentMap[Partition, Dependency] = {

      CachingUtils.ConcurrentMap()
    }
  }

  val existingBroadcast: Broadcast[Existing] = {

    sparkContext.broadcast(Existing())
  }
  def existing: ConcurrentMap[Partition, Dependency] = existingBroadcast.value.self

  def findDependency(p: Partition): Dependency = {

    val result = existing.getOrElseUpdate(
      p,
      Dependency(p)
    )

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    val dep = findDependency(split).WTask(context)

    dep.output(0) // TODO: start should be customisable
  }
}

object IncrementallyCachedRDD {

//  class IteratorWithCounter[T](private val _self: Iterator[T]) {
//
//    val counter = new AtomicInteger(0)
//    def traversedIndex: Int = counter.get()
//
//    val self: Iterator[T] = _self.map { v =>
//      counter.getAndIncrement()
//      v
//    }
//  }
}
