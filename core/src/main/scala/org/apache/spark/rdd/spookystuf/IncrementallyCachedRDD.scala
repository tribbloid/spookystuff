package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.utils.CachingUtils
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.spookystuf.ExternalAppendOnlyArray
import org.apache.spark.{Partition, TaskContext}

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

      val counter = new AtomicInteger(0)
      def traversedIndex: Int = counter.get()

      val compute: Iterator[T] = firstParent[T].compute(p, task).map { v =>
        counter.getAndIncrement()
        v
      }

      def getOrCompute(start: Int): Iterator[T] = {

        cache.getOrComputeIterator(compute, traversedIndex, start)
      }

      def active: WTask = Option(_activeTask).getOrElse {
        regenerate()
        this
      }

      def regenerate(): Unit = {

        _activeTask = this
      }

      def output(start: Int): Iterator[T] = {

        val activeTask = active.task
        val thisTask = this.task

        if (!this.eq(active)) {
          val taskMetrics = activeTask.taskMetrics()
          val activeAccs = taskMetrics.accumulators()

          for (acc <- activeAccs) {

            acc.reset()
            thisTask.registerAccumulator(acc)
          }
        }

        try {

          active.getOrCompute(start)
        } catch {
          case _: ArrayIndexOutOfBoundsException =>
            regenerate()

            getOrCompute(start)
        }
      }
    }

  }

  val existingBroadcast: Broadcast[ConcurrentMap[Partition, Dependency]] = {

    val v: CachingUtils.ConcurrentMap[Partition, Dependency] = {

      CachingUtils.ConcurrentMap()
    }

    sparkContext.broadcast(v)
  }
  def existing: ConcurrentMap[Partition, Dependency] = existingBroadcast.value

  def findDependency(p: Partition): Dependency = {

    val result = existing.getOrElseUpdate(
      p,
      Dependency(p)
    )

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    val dep = findDependency(split).WTask(context)

    val oldCtx = dep.task
    val newCtx = context

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
