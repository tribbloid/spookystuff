package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore

import com.tribbloids.spookystuff.utils.{CachingUtils, IDMixin}
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{OneToOneDependency, Partition, SparkEnv, TaskContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

case class IncrementallyCachedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
    serializerFactory: () => Serializer = () => SparkEnv.get.serializer
) extends RDD[T](
      prev.sparkContext,
      Nil // useless, already have overridden getDependencies
    )
    with Logging {

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
      serializerFactory
    )

    @volatile var _activeTask: InTask = _

    case class InTask(taskContext: TaskContext) extends IDMixin {

      {
        if (_activeTask == null) _activeTask = this
      }

      override protected lazy val _id: Any = taskContext.taskAttemptId()

      @volatile var _commissionedBy: TaskContext = taskContext

      lazy val accumulatorMap: Map[Long, AccumulatorV2[_, _]] = {

        val metrics = taskContext.taskMetrics()
        val activeAccs = metrics.accumulators()
        Map(activeAccs.map(v => v.id -> v): _*)
      }

      def recommission(neo: InTask): Unit = {

        val newTask = neo.taskContext

        semaphore.acquire()

        val transferAccums = !_commissionedBy.eq(newTask)

        if (transferAccums) {

          for ((_, acc) <- accumulatorMap) {

            acc.reset()
          }

          logInfo(s"recommissioning ${taskContext.taskAttemptId()}: ${_commissionedBy
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
                  logWarning(
                    s"Accumulator ${newAcc.toString()} cannot be found in task ${taskContext.stageId()}/${taskContext.taskAttemptId()}"
                  )
              }
            }
          }

          semaphore.release()
        }
      }

      lazy val semaphore = new Semaphore(1) // cannot be shared by >1 threads

      lazy val compute: ConsumedIterator.Wrap[T] = {

        val raw = firstParent[T].compute(p, taskContext)
        ConsumedIterator.wrap(raw)
      }

      def active: InTask = Option(_activeTask).getOrElse {
        activate()
        this
      }

      def activate(): Unit = {

        logWarning(
          s"regenerating partition ${p.index} : ${_activeTask.taskContext.taskAttemptId()} -> ${taskContext.taskAttemptId()}")
        _activeTask = this
      }

      def output(start: Int): Iterator[T] = {

        val result = try {
          active.recommission(this)

          cacheArray
            .StartingFrom(start)
            .CachedOrComputeIterator {

              object ActiveOrRegeneratedIterator extends FallbackIterator[T] {

                @volatile override var primary: Iterator[T] with ConsumedIterator = active.compute

                override lazy val backup: Iterator[T] with ConsumedIterator = {
                  val result = InTask.this.compute

                  activate()

                  result
                }

                override protected def _primaryHasNext: Option[Boolean] = {

                  try {
                    Some(primary.hasNext)
                  } catch {
                    case e: Throwable =>
                      logError(s"Partition ${p.index} from ${active.taskContext} is broken, recomputing: $e")
                      logDebug("", e)

                      None
                  }

                }
              }

              ActiveOrRegeneratedIterator

            }

        } catch {
          case e: ExternalAppendOnlyArray.CannotComputeException =>
            logError(s"Partition ${p.index} from ${active.taskContext} cannot be used, recomputing: $e")
            logDebug("", e)

            val result = cacheArray
              .StartingFrom(start)
              .CachedOrComputeIterator(
                this.compute
              )

            activate()

            result
        }

        result
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

    val dep = findDependency(split).InTask(context)

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
}
