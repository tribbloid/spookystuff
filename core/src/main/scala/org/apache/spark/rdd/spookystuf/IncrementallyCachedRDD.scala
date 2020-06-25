package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.utils.CachingUtils
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.spookystuf.ExternalAppendOnlyArray
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{OneToOneDependency, Partition, SparkEnv, TaskContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

case class IncrementallyCachedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
    serializer: Serializer = SparkEnv.get.serializer
) extends RDD[T](
      prev.sparkContext,
      Nil // useless, already have overridden getDependencies
    ) {

  import IncrementallyCachedRDD._

  /**
    * mimicking DAGScheduler.cacheLocs, but using accumulator as I don't mess with BlockManager
    */
  val cacheLocAccum: MapAccumulator[Int, Seq[TaskLocation]] = MapAccumulator()
  cacheLocAccum.register(sparkContext, Some(this.getClass.getSimpleName))

  @transient var prevWithLocations: RDD[T] = prev.mapPartitions { itr =>
    val pid = TaskContext.get.partitionId
    val bm = SparkEnv.get.blockManager.blockManagerId
    val loc = TaskLocation(bm.host, bm.executorId)
    cacheLocAccum.add(pid -> Seq(loc))
    itr
  }

  override def getDependencies: Seq[spark.Dependency[_]] = {

    Seq(new OneToOneDependency(prevWithLocations))
  }

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  case class Dependency(
      p: Partition
  ) extends NOTSerializable {

    val cacheArray = new ExternalAppendOnlyArray[T](
      s"${this.getClass.getSimpleName}-${p.index}",
      storageLevel,
      serializer
    )

    @volatile var _activeTask: WTask = _

    case class WTask(task: TaskContext) {

      @volatile var _commissionedBy: TaskContext = task

      lazy val accumulatorMap: Map[Long, AccumulatorV2[_, _]] = {

        val metrics = task.taskMetrics()
        val activeAccs = metrics.accumulators()
        Map(activeAccs.map(v => v.id -> v): _*)
      }

      def recommission(neo: WTask): Unit = {

        val newTask = neo.task

        semaphore.acquire()

        val transferAccums = !_commissionedBy.eq(newTask)

        if (transferAccums) {

          for ((_, acc) <- accumulatorMap) {

            acc.reset()
          }

          LoggerFactory
            .getLogger(this.getClass)
            .info(s"recommissioning ${task.taskAttemptId()}: ${_commissionedBy
              .taskAttemptId()} -> ${newTask
              .taskAttemptId()}, accumulators ${accumulatorMap.keys.map(v => "#" + v).mkString(",")}")

          _commissionedBy = newTask
        }

        newTask.addTaskCompletionListener[Unit] { _: TaskContext =>
          if (transferAccums) {

            val newAccums = neo.accumulatorMap

            for ((k, newAcc) <- newAccums) {

              accumulatorMap.get(k) match {

                case Some(oldAcc) =>
                  newAcc.asInstanceOf[AccumulatorV2[Any, Any]].merge(oldAcc.asInstanceOf[AccumulatorV2[Any, Any]])
                case None =>
                  LoggerFactory
                    .getLogger(this.getClass)
                    .warn(
                      s"Accumulator ${newAcc.toString()} cannot be found in task ${task.stageId()}/${task.taskAttemptId()}"
                    )
              }
            }
          }

          semaphore.release()
        }
      }

      val semaphore = new Semaphore(1) // cannot be shared by >1 threads

      val counter = new AtomicInteger(0)

      val compute: Iterator[T] = firstParent[T].compute(p, task).map { v =>
        counter.getAndIncrement()
        v
      }

      def getOrCompute(start: Int): Iterator[T] = {

        cacheArray.StartingFrom(start).CachedOrComputeIterator(compute, counter.get())
      }

      def active: WTask = Option(_activeTask).getOrElse {
        regenerate()
        this
      }

      def regenerate(): Unit = {

        _activeTask = this
      }

      def output(start: Int): Iterator[T] = {

        try {

          active.recommission(this)
          active.getOrCompute(start)
        } catch {
          case e: ExternalAppendOnlyArray.CannotComputeException =>
            LoggerFactory.getLogger(this.getClass).warn(s"recomputing partition ${p.index} as ${e.getMessage}")
            regenerate()
            getOrCompute(start)
        }
      }
    }
  }

  val existingBroadcast: Broadcast[Existing[Dependency]] = {

    sparkContext.broadcast(Existing())
  }
  def existing: ConcurrentMap[Int, Dependency] = existingBroadcast.value.self

  def findDependency(p: Partition): Dependency = {

    val result = existing.getOrElseUpdate(
      p.index,
      Dependency(p)
    )

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    val dep = findDependency(split).WTask(context)

    dep.output(0) // TODO: start should be customisable
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
    prevWithLocations = null
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {

//    val key = this.id -> split.index
//    println(key.toString())

    val cacheLocs = cacheLocAccum.value

    val result = cacheLocs
      .getOrElse(
        split.index,
        Nil
      )
      .map { loc =>
        loc.toString
      }

    result
  }
}

object IncrementallyCachedRDD {

  case class Existing[T]() {

    @transient lazy val self: CachingUtils.ConcurrentMap[Int, T] = {

      CachingUtils.ConcurrentMap()
    }
  }

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
