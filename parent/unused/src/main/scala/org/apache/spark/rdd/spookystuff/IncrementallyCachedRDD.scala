package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.unused.ExternalAppendOnlyArray
import com.tribbloids.spookystuff.utils.Caching.ConcurrentMap
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, LocalCleanable}
import com.tribbloids.spookystuff.utils.{Caching, EqualBy, Retry, SCFunctions}
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

import java.util.concurrent.Semaphore
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
      p: Partition,
      taskID: Long
  ) extends LocalCleanable {

    lazy val externalCacheArray: ExternalAppendOnlyArray[T] = ExternalAppendOnlyArray[T](
      s"${this.getClass.getSimpleName}-$id-${p.index}-$taskID",
      incrementalStorageLevel,
      serializerFactory
    )

    @volatile var _active: InTask = _

    case class InTask(taskCtx: TaskContext) extends EqualBy {

      //      {'
      //        if (_activeTask == null) _activeTask = this
      //      }

      lazy val semaphore: Semaphore = new Semaphore(1) // cannot be shared by >1 threads

      override protected lazy val _equalBy: Any = taskCtx.taskAttemptId()

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
              case e: Exception =>
                logError(
                  s"Partition ${p.index} in task ${active.taskCtx
                      .taskAttemptId()} - stage ${active.taskCtx
                      .stageId()} is broken at ${_primary.offset}, fallback to use ${_backup.getClass}\n" +
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

    override def _lifespan: Lifespan = Lifespan.JVM.apply()

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

  val depCacheLocalRef: Broadcast[DepCache[Dependency]] = {

    sparkContext.broadcast(DepCache[Dependency](this.id))
  }

  private def depCache: DepCache[Dependency] = {
    depCacheLocalRef.value
  }

  def findDependency(p: Partition): Dependency = {

    val depCache = this.depCache

    val result = depCache.getOrCreate(p.index) {

      val taskContext = TaskContext.get()
      val taskID = taskContext.taskAttemptId()

      logDebug(s"new Dependency ${p.index} in task $taskID")
      Dependency(p, taskID)
    }

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    Retry.ExponentialBackoff(3, 1000) {
      // unpersistIncremental relies on scheduler and may not execute immediately, in this case

      try {

        {

          var result = findDependency(split)
          if (result.externalCacheArray.isCleaned) {

            depCache.drop(split.index)
            result = findDependency(split)
          }
          result
        }

        val inTask = findDependency(split).InTask(context)
        val result = inTask.cachedOrCompute() // TODO: start should be customisable
        return result
      } catch {
        case e: Exception =>
          depCache.drop(split.index)

          throw e
      }
    }

    sys.error("IMPOSSIBLE!")
  }

  override def clearDependencies(): Unit = {
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

    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} cannot be unpersisted, please use clearIncrementalCache() instead"
    )
  }

  def clearIncrementalCache(): Array[Boolean] = {
    import com.tribbloids.spookystuff.utils.SpookyViews._

    val info = s"Incremental cache cleanup - RDD $id"

    logInfo(info)

    val result = SCFunctions(sparkContext).withJob(info) {

      this
        .mapOncePerWorker { _ =>
          logInfo(info + s" - executor ${SparkHelper.taskLocationStrOpt.getOrElse("??")}")
          val result = depCache.cleanUp()

          result
        }
        .collect()
    }

    depCacheLocalRef.unpersist()

    result
  }
}

object IncrementallyCachedRDD {

  import com.tribbloids.spookystuff.utils.CommonViews._

  case class DepCache[T <: Cleanable](
      rddID: Int
  ) {

    @transient lazy val existing: Caching.ConcurrentMap[Int, T] =
      Caching.ConcurrentMap()

    def getOrCreate(id: Int)(create: => T): T = {

      existing.getOrElseUpdateSynchronously(id) {

        create
      }
    }

    def drop(id: Int): this.type = {

      existing.get(id).foreach { v =>
        v.clean()
      }

      existing -= id

      this
    }

    def cleanUp(): Boolean = existing.synchronized {

      val map = existing

      if (map.nonEmpty) {

        val keys = map.keys

        keys.foreach { k =>
          drop(k)
        }

        // TODO: This will render all CachedIterators to be broken and force all
        // CacheOrComputeActiveOrComputeFromScratch to use the last resort
        // is it possible to improve?
      }

      true
    }
  }
}
