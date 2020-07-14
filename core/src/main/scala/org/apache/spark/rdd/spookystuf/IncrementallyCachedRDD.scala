package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, IDMixin, SCFunctions}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.dsl.utils.LazyVar
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.utils.SparkHelper
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
  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * mimicking DAGScheduler.cacheLocs, but using accumulator as I don't mess with BlockManager
    */
  val cacheLocAccum: MapAccumulator[Int, Seq[TaskLocation]] = MapAccumulator()
  cacheLocAccum.register(sparkContext, Some(this.getClass.getSimpleName))

  @transient var prevWithLocations: RDD[T] = prev.mapPartitions { itr =>
    val pid = TaskContext.get.partitionId
    val loc = SparkHelper.taskLocationOpt.get

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

    val externalCacheArray = new ExternalAppendOnlyArray[T](
      s"${this.getClass.getSimpleName}-${p.index}",
      incrementalStorageLevel,
      serializerFactory
    )

    @volatile var _active: InTask = _

    case class InTask(taskCtx: TaskContext) extends IDMixin {

      //      {
      //        if (_activeTask == null) _activeTask = this
      //      }

      lazy val semaphore = new Semaphore(1) // cannot be shared by >1 threads

      override protected lazy val _id: Any = taskCtx.taskAttemptId()

      lazy val uncleanTask: UncleanTaskContext = UncleanTaskContext(taskCtx)

      lazy val accumulatorMap: Map[Long, AccumulatorV2[_, _]] = {

        val metrics = taskCtx.taskMetrics()
        val activeAccs = metrics.accumulators()
        Map(activeAccs.map(v => v.id -> v): _*)
      }

      def getActive: InTask = Dependency.this.synchronized {
        Option(_active).getOrElse {
          activate()
          this
        }
      }

      def activate(): Unit = Dependency.this.synchronized {

        if (_active != this) {

          Option(_active).foreach { v =>
            v.uncleanTask.close()
          }

          Option(_active).foreach { aa =>
            logWarning(
              s"regenerating partition ${p.index} : ${aa.taskCtx.taskAttemptId()} -> ${taskCtx.taskAttemptId()}"
            )
          }

          _active = this
        }
      }

      // can only be used once, otherwise have to recreate from self task instead of active one
      lazy val doCompute: LazyVar[ConsumedIterator.Wrap[T]] = LazyVar {

        activate()

        val raw = firstParent[T].compute(p, uncleanTask)
        val result = ConsumedIterator.wrap(raw)

        result
      }

      val _commissionHistory: mutable.Set[InTask] = mutable.Set(this)

      def commissionedBy(by: InTask): Commissioned = Commissioned(this, by)
      lazy val selfCommissioned: Commissioned = commissionedBy(this)

      def cachedOrCompute(start: Int = 0): Iterator[T] = {

        externalCacheArray.sanity()

        val active = getActive
        val commissioned = active.commissionedBy(this)

        val cacheOrComputeActive = externalCacheArray
          .StartingFrom(start)
          .CachedOrComputeIterator { () =>
            commissioned.compute
          }

        object CacheOrComputeActiveOrComputeFromScratch extends FallbackIterator[T] {

          override def getPrimary: Iterator[T] with ConsumedIterator = cacheOrComputeActive

          override def getBackup: Iterator[T] with ConsumedIterator = {

            val recompute = selfCommissioned.recompute

            recompute
          }

          override protected def _primaryHasNext: Option[Boolean] = {

            // if cacheOrComputeActive successfully reached end of line there is no need to use backup
            try {
              Some(_primary.hasNext)
            } catch {
              case e: Throwable =>
                logError(
                  s"Partition ${p.index} in task ${active.taskCtx
                    .taskAttemptId()} - stage ${active.taskCtx
                    .stageId()} is broken at ${_primary.offset}, falling back to use ${_backup.getClass}\n" +
                    s"cause by $e"
                )
                logDebug("", e)

                None
            }
          }
        }

        CacheOrComputeActiveOrComputeFromScratch
      }
    }

    object Commissioned {

      lazy val existing: ConcurrentMap[(InTask, InTask), Commissioned] = ConcurrentMap()

      def apply(from: InTask, by: InTask): Commissioned = {

        val result = existing.getOrElseUpdateSynchronously(from -> by) {

          new Commissioned(from, by)
        }
        result.initializeOnce

        result
      }
    }

    class Commissioned(from: InTask, by: InTask) {

      def fromTask: TaskContext = from.taskCtx

      lazy val semaphoreAcquiredOnce: Unit =
        from.semaphore.acquire()

      lazy val initializeOnce: Unit = {
        semaphoreAcquiredOnce

        val byTask = by.taskCtx

        by.taskCtx.addTaskCompletionListener[Unit] { _: TaskContext =>
          from.semaphore.release()
        }

        val needToTransferAccums = !from._commissionHistory.contains(by)

        if (needToTransferAccums) {

          for ((_, acc) <- from.accumulatorMap) {

            acc.reset()
          }

          logDebug(s"recommissioning ${fromTask.taskAttemptId()}: -> ${byTask
            .taskAttemptId()}, accumulators ${from.accumulatorMap.keys.map(v => "#" + v).mkString(",")}")

          from._commissionHistory += by
        }

        byTask.addTaskCompletionListener[Unit] { _: TaskContext =>
          if (needToTransferAccums) {

            val newAccums = by.accumulatorMap

            for ((k, newAcc) <- newAccums) {

              from.accumulatorMap.get(k) match {

                case Some(oldAcc) =>
                  newAcc.asInstanceOf[AccumulatorV2[Any, Any]].merge(oldAcc.asInstanceOf[AccumulatorV2[Any, Any]])
                case None =>
                  logDebug(
                    s"Accumulator ${newAcc.toString()} cannot be found in task ${fromTask
                      .stageId()}/${fromTask.taskAttemptId()}"
                  )
              }
            }
          }

        }
      }

      def compute: ConsumedIterator.Wrap[T] = from.doCompute.value

      // compute from scratch using current task instead of commissioned one, always succeed
      def recompute: ConsumedIterator.Wrap[T] = {
        from.doCompute.regenerate

      }
    }

    override def _lifespan: Lifespan = Lifespan.JVM()

    /**
      * can only be called once
      */
    override protected def cleanImpl(): Unit = {

      Option(_active).foreach { v =>
        v.uncleanTask.close()
      }

      externalCacheArray.clean()
    }
  }

  val existingBroadcast: Broadcast[ExistingOnWorkers[Dependency]] = {

    sparkContext.broadcast(ExistingOnWorkers(this.id))
  }
  def existing: ExistingOnWorkers[Dependency] = existingBroadcast.value

  def findDependency(p: Partition): Dependency = this.synchronized {

    val result = existing.self.getOrElseUpdateSynchronously(p.index) {
      val taskContext = TaskContext.get()

      logDebug(s"new Dependency ${p.index} in task ${taskContext.taskAttemptId()}")
      Dependency(p)
    }

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    while (true) {
      // unpersistIncremental relies on scheduler and may not execute immediately, in this case

      try {

        val inTask = findDependency(split).InTask(context)

        return inTask.cachedOrCompute() // TODO: start should be customisable
      } catch {
        case e: Throwable =>
          existing.self -= split.index
      }
    }

    sys.error("IMPOSSIBLE!")
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

    unpersistIncremental()

    this
  }

  protected def unpersistIncremental(): Array[Boolean] = {
    import com.tribbloids.spookystuff.utils.SpookyViews._

    val info = s"Incremental cache cleanup - RDD $id"

    logInfo(info)

    SCFunctions(sparkContext).withJob(info) {

      this
        .mapOncePerWorker { v =>
          logInfo(info + s" - executor ${SparkHelper.taskLocationStrOpt.getOrElse("??")}")
          val result = existing.cleanUp()

          result
        }
        .collect()
    }
  }
}

object IncrementallyCachedRDD {

  case class ExistingOnWorkers[T <: LocalCleanable](id: Int) extends Logging {

    @transient lazy val _map: CachingUtils.ConcurrentMap[Int, T] = {

      CachingUtils.ConcurrentMap()
    }

    def cleanUp(): Boolean = synchronized {

      val map = this.self

      if (map.nonEmpty) {

        val values = map.values.toList

        map.clear()

        values.foreach { v =>
          v.clean()
        }
        //TODO: This will render all CachedIterators to be broken and force all
        // CacheOrComputeActiveOrComputeFromScratch to use the last resort
        // is it possible to improve?
      }

      true
    }

    def self: ConcurrentMap[Int, T] = {

      _map
    }
  }
}
