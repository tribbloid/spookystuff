package org.apache.spark.rdd.spookystuf

import java.util.concurrent.Semaphore

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, IDMixin}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.dsl.utils.LazyVar
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

        val thatTask = that.self

        semaphore.acquire()

        that.self.addTaskCompletionListener[Unit] { _: TaskContext =>
          semaphore.release()
        }

        val needToTransferAccums = !_commissionedBy.contains(that)

        if (needToTransferAccums) {

          for ((_, acc) <- accumulatorMap) {

            acc.reset()
          }

          logDebug(s"recommissioning ${self.taskAttemptId()}: -> ${thatTask
            .taskAttemptId()}, accumulators ${accumulatorMap.keys.map(v => "#" + v).mkString(",")}")

          _commissionedBy += that
        }

        thatTask.addTaskCompletionListener[Unit] { _: TaskContext =>
          if (needToTransferAccums) {

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
      lazy val compute: LazyVar[ConsumedIterator.Wrap[T]] = LazyVar {

        activate()

        val raw = firstParent[T].compute(p, uncleanSelf)
        val result = ConsumedIterator.wrap(raw)

        result
      }

      // compute from scratch using current task instead of commissioned one, always succeed
      def recompute: ConsumedIterator.Wrap[T] = {
        compute.regenerate
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

        cacheArray.sanity()

        getActive.recommission(this)

        val cacheOrComputeActive = cacheArray
          .StartingFrom(start)
          .CachedOrComputeIterator { () =>
            getActive.compute.value
          }

        object CacheOrComputeActiveOrComputeFromScratch extends FallbackIterator[T] {

          override def getPrimary: Iterator[T] with ConsumedIterator = cacheOrComputeActive

          override def getBackup: Iterator[T] with ConsumedIterator = InTask.this.recompute

          override protected def _primaryHasNext: Option[Boolean] = {

            // if cacheOrComputeActive successfully reached end of line there is no need to use backup
            try {
              Some(_primary.hasNext)
            } catch {
              case e: Throwable =>
                logError(s"Partition ${p.index} from ${getActive.self} is broken, recomputing: $e")
                logDebug("", e)

                None
            }
          }
        }

        CacheOrComputeActiveOrComputeFromScratch

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

    sparkContext.broadcast(Existing(this.id))
  }
  def existing: Existing[Dependency] = existingBroadcast.value

  def findDependency(p: Partition): Dependency = this.synchronized {

    val result = existing.map.getOrElseUpdate(
      p.index, {
        val taskContext = TaskContext.get()

        logDebug(s"new Dependency ${p.index} in task ${taskContext.taskAttemptId()}")
        Dependency(p)
      }
    )

    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    while (true) {
      // unpersistIncremental relies on scheduler and may not execute immediately, in this case

      try {

        val dep = findDependency(split).InTask(context)

        return dep.cachedOrCompute() // TODO: start should be customisable
      } catch {
        case e: Throwable =>
          existing.map -= split.index
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

    logInfo(s"Cleaning incremental caches of RDD ${this.id}")

    this
      .mapOncePerWorker { v =>
        logInfo(s"Cleaning incremental cache of RDD $id")
        val result = existing.cleanUp()

        result
      }
      .collect()
  }
}

object IncrementallyCachedRDD {

  case class Existing[T <: LocalCleanable](id: Int) extends Logging {

    @transient lazy val _map: CachingUtils.ConcurrentMap[Int, T] = {

      CachingUtils.ConcurrentMap()
    }

    def cleanUp(): Boolean = synchronized {

      val map = this.map

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

    def map: ConcurrentMap[Int, T] = {

      _map
    }
  }
}
