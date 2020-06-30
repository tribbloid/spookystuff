package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore

import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, IDMixin}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{OneToOneDependency, Partition, SparkEnv, TaskContext}

import scala.collection.mutable
import scala.reflect.ClassTag

case class IncrementallyCachedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    incrementalStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
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
  ) extends LocalCleanable {

    val cacheArray = new ExternalAppendOnlyArray[T](
      s"${this.getClass.getSimpleName}-${p.index}",
      incrementalStorageLevel,
      serializerFactory
    )

    @volatile var _active: InTask = _
    lazy val semaphore = new Semaphore(1) // cannot be shared by >1 threads

    case class InTask(self: TaskContext) extends IDMixin {

//      {
//        if (_activeTask == null) _activeTask = this
//      }

      override protected lazy val _id: Any = self.taskAttemptId()

      lazy val uncleanSelf: UncleanTaskContext = UncleanTaskContext(self)

      val _commissionedBy: mutable.Set[InTask] = mutable.Set(this)

      lazy val accumulatorMap: Map[Long, AccumulatorV2[_, _]] = {

        val metrics = self.taskMetrics()
        val activeAccs = metrics.accumulators()
        Map(activeAccs.map(v => v.id -> v): _*)
      }

      def recommission(that: InTask): Unit = {

        val newTask = that.self

        semaphore.acquire()

        that.self.addTaskCompletionListener { v =>
          semaphore.release()
        }

        val transferAccums = !_commissionedBy.contains(that)

        if (transferAccums) {

          for ((_, acc) <- accumulatorMap) {

            acc.reset()
          }

          logDebug(s"recommissioning ${self.taskAttemptId()}: -> ${newTask
            .taskAttemptId()}, accumulators ${accumulatorMap.keys.map(v => "#" + v).mkString(",")}")

          _commissionedBy += that
        }

        newTask.addTaskCompletionListener[Unit] { _: TaskContext =>
          if (transferAccums) {

            val newAccums = that.accumulatorMap

            for ((k, newAcc) <- newAccums) {

              accumulatorMap.get(k) match {

                case Some(oldAcc) =>
                  newAcc.asInstanceOf[AccumulatorV2[Any, Any]].merge(oldAcc.asInstanceOf[AccumulatorV2[Any, Any]])
                case None =>
                  logDebug(
                    s"Accumulator ${newAcc.toString()} cannot be found in task ${self.stageId()}/${self.taskAttemptId()}"
                  )
              }
            }
          }

        }
      }

      // can only be used once, otherwise have to recreate from self task instead of active one
      lazy val compute: ConsumedIterator.Wrap[T] = {

        activateThis()

        val raw = firstParent[T].compute(p, uncleanSelf)
        ConsumedIterator.wrap(raw)
      }

      def active: InTask = Dependency.this.synchronized {
        Option(_active).getOrElse {
          activateThis()
          this
        }
      }

      def activateThis(): Unit = Dependency.this.synchronized {

        if (_active != this) {

          Option(_active).foreach { v =>
            v.uncleanSelf.close()
          }

          Option(_active).foreach { aa =>
            logWarning(
              s"regenerating partition ${p.index} : ${aa.self.taskAttemptId()} -> ${self.taskAttemptId()}"
            )
          }

          _active = this
        }
      }

      def cachedOrCompute(start: Int = 0): Iterator[T] = {

        val result = try {
          active.recommission(this)

          cacheArray
            .StartingFrom(start)
            .CachedOrComputeIterator {

              // TODO: this consistently fails if checkpointed partition was missing, and there is no way to test it
              object ActiveOrRegeneratedIterator extends FallbackIterator[T] {

                @volatile override var primary: Iterator[T] with ConsumedIterator = active.compute

                override lazy val backup: Iterator[T] with ConsumedIterator = {
                  InTask.this.compute
                }

                override protected def _primaryHasNext: Option[Boolean] = {

                  try {
                    Some(primary.hasNext)
                  } catch {
                    case e: Throwable =>
                      logError(s"Partition ${p.index} from ${active.self} is broken, recomputing: $e")
                      logDebug("", e)

                      None
                  }

                }
              }

              ActiveOrRegeneratedIterator

            }

        } catch {
          case e: ExternalAppendOnlyArray.CannotComputeException =>
            logError(s"Partition ${p.index} from ${active.self} cannot be used, recomputing: $e")
            logDebug("", e)

            val result = cacheArray
              .StartingFrom(start)
              .CachedOrComputeIterator {
                InTask.this.compute
              }

            activateThis()

            result
        }

        result
      }

    }

    override def _lifespan: Lifespan = Lifespan.JVM()

    /**
      * can only be called once
      */
    override protected def cleanImpl(): Unit = {

      Option(_active).foreach { v =>
        v.uncleanSelf.close()
      }

      cacheArray.clean()
    }
  }

  val existingBroadcast: Broadcast[Existing[Dependency]] = {

    sparkContext.broadcast(Existing())
  }
  def existing: Existing[Dependency] = existingBroadcast.value

  def findDependency(p: Partition): Dependency = {

    val result = existing.map.getOrElseUpdate(
      p.index,
      Dependency(p)
    )

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    val dep = findDependency(split).InTask(context)

    dep.cachedOrCompute() // TODO: start should be customisable
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

  override def unpersist(blocking: Boolean): this.type = {

    super.unpersist(blocking)

    this.foreachPartition { v =>
      existing.cleanAll()
    }

    this
  }
}

object IncrementallyCachedRDD {

  case class Existing[T <: LocalCleanable]() {

    @transient lazy val map: CachingUtils.ConcurrentMap[Int, T] = {

      CachingUtils.ConcurrentMap()
    }

    def cleanAll(): this.type = synchronized {

      map.foreach {
        case (k, v) =>
          v.clean()
      }

      map.clear()

      this
    }
  }
}
