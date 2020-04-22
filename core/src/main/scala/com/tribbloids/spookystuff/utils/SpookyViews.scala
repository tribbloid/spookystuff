package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.execution.ScratchRDDs
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.locality.PartitionIdPassthrough
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuf.NarrowDispersedRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.{RDDInfo, StorageLevel}
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.ListMap
import scala.collection.{Map, TraversableLike}
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by peng on 11/7/14.
  * implicit conversions in this package are used for development only
  */
object SpookyViewsSingleton {

  val SPARK_JOB_DESCRIPTION = "spark.job.description"
  val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  // (stageID -> threadID) -> isExecuted
  val perCoreMark: ConcurrentMap[(Int, Long), Boolean] = ConcurrentMap()
  // stageID -> isExecuted
  val perWorkerMark: ConcurrentMap[Int, Boolean] = ConcurrentMap()

  // large enough such that all idle threads has a chance to pick up >1 partition
  val REPLICATING_FACTOR = 16
}

abstract class SpookyViews extends CommonViews {

  import SpookyViewsSingleton._

  implicit class SparkContextView(self: SparkContext) extends SCFunctions(self)

  implicit class RDDView[T](val self: RDD[T]) extends RDDViewBase[T] {

    def sc: SparkContext = self.sparkContext

    def collectPerPartition: Array[List[T]] =
      self
        .mapPartitions(
          v => Iterator(v.toList)
        )
        .collect()

    def multiPassMap[U: ClassTag](f: T => Option[U]): RDD[U] = {

      multiPassFlatMap(f.andThen(v => v.map(Traversable(_))))
    }

    //if the function returns None for it will be retried as many times as it takes to get rid of them.
    //core problem is optimization: how to SPILL properly and efficiently?
    //TODO: this is the first implementation, simple but may not the most efficient
    def multiPassFlatMap[U: ClassTag](f: T => Option[TraversableOnce[U]]): RDD[U] = {

      val counter = sc.longAccumulator
      var halfDone: RDD[Either[T, TraversableOnce[U]]] = self.map(v => Left(v))

      while (true) {
        counter.reset()

        val updated: RDD[Either[T, TraversableOnce[U]]] = halfDone.map {
          case Left(src) =>
            f(src) match {
              case Some(res) => Right(res)
              case None =>
                counter add 1L
                Left(src)
            }
          case Right(res) => Right(res)
        }

        updated.persist().count()
        halfDone.unpersist()

        if (counter.value == 0) return updated.flatMap(_.right.get)

        halfDone = updated
      }
      sys.error("impossible")

      //      self.mapPartitions{
      //        itr =>
      //          var intermediateResult: Iterator[Either[T, TraversableOnce[U]]] = itr.map(v => Left(v))
      //
      //          var unfinished = true
      //          while (unfinished) {
      //
      //            var counter = 0
      //            val updated: Iterator[Either[T, TraversableOnce[U]]] = intermediateResult.map {
      //              case Left(src) =>
      //                f(src) match {
      //                  case Some(res) => Right(res)
      //                  case None =>
      //                    counter = counter + 1
      //                    Left(src)
      //                }
      //              case Right(res) => Right(res)
      //            }
      //            intermediateResult = updated
      //
      //            if (counter == 0) unfinished = false
      //          }
      //
      //          intermediateResult.flatMap(_.right.get)
      //      }
    }

    //    def persistDuring[T](newLevel: StorageLevel, blocking: Boolean = true)(fn: => T): T =
    //      if (self.getStorageLevel == StorageLevel.NONE){
    //        self.persist(newLevel)
    //        val result = fn
    //        self.unpersist(blocking)
    //        result
    //      }
    //      else {
    //        val result = fn
    //        self.unpersist(blocking)
    //        result
    //      }

    //  def checkpointNow(): Unit = { TODO: is it useless now?
    //    persistDuring(StorageLevel.MEMORY_ONLY) {
    //      self.checkpoint()
    //      self.foreach(_ =>)
    //      self
    //    }
    //    Unit
    //  }

    def injectPassthroughPartitioner: RDD[(Int, T)] = {

      val withPID = self.map(v => TaskContext.get().partitionId() -> v)
      val result = withPID.partitionBy(new PartitionIdPassthrough(withPID.partitions.length))
      result
    }

    def storageInfo: RDDInfo = {
      val rddInfos = sc.getRDDStorageInfo
      rddInfos.find(_.id == self.id).get
    }

    def isPersisted: Boolean = {
      storageInfo.storageLevel != StorageLevel.NONE
    }

    def assertIsBeacon(): Unit = {
      assert(isPersisted)
      assert(self.isEmpty())
    }

    def shufflePartitions: RDD[T] = {

      val randomKeyed: RDD[(Long, T)] = self.keyBy(_ => Random.nextLong())
      val shuffled = randomKeyed.partitionBy(new HashPartitioner(self.partitions.length))
      shuffled.values
    }

    /**
      *
      * @param f function applied on each element
      * @tparam R result type
      * @return
      */
    def mapOncePerCore[R: ClassTag](f: T => R): RDD[R] = {

      self.mapPartitions { itr =>
        val stageID = TaskContext.get.stageId()
        //          val executorID = SparkEnv.get.executorId //this is useless as perCoreMark is a local singleton
        val threadID = Thread.currentThread().getId
        val allIDs = stageID -> threadID
        val alreadyRun = perCoreMark.synchronized {
          val alreadyRun = perCoreMark.getOrElseUpdate(allIDs, false)
          if (!alreadyRun) {
            perCoreMark.put(allIDs, true)
          }
          alreadyRun
        }
        if (!alreadyRun) {
          val result = f(itr.next())
          //            Thread.sleep(1000)
          Iterator(result)
        } else {
          Iterator.empty
        }
      }
    }

    def mapOncePerWorker[R: ClassTag](f: T => R): RDD[R] = {

      self.mapPartitions { itr =>
        val stageID = TaskContext.get.stageId()
        val alreadyRun = perWorkerMark.synchronized {
          val alreadyRun = perWorkerMark.getOrElseUpdate(stageID, false)
          if (!alreadyRun) {
            perWorkerMark.put(stageID, true)
          }
          alreadyRun
        }
        if (!alreadyRun) {
          val result = f(itr.next())
          //            Thread.sleep(1000)
          Iterator(result)
        } else {
          Iterator.empty
        }
      }
    }

    case class Disperse(
        pSizeGen: Int => Long = _ => Long.MaxValue,
        scratchRDD: ScratchRDDs = ScratchRDDs()
    ) {

      def asRDD(numPartitions: Int): RDD[T] = {

        val result = new NarrowDispersedRDD(
          self,
          numPartitions,
          NarrowDispersedRDD.ByRange(pSizeGen)
        )
        result
      }
    }
  }

  implicit class StringRDDView(val self: RDD[String]) {

    //csv has to be headerless, there is no better solution as header will be shuffled to nowhere
    def csvToMap(headerRow: String, splitter: String = ","): RDD[Map[String, String]] = {
      val headers = headerRow.split(splitter)

      //cannot handle when a row is identical to headerline, but whatever
      self.map { str =>
        {
          val values = str.split(splitter)

          ListMap(headers.zip(values): _*)
        }
      }
    }

    def tsvToMap(headerRow: String): RDD[Map[String, String]] = csvToMap(headerRow, "\t")
  }

  implicit class PairRDDView[K: ClassTag, V: ClassTag](val self: RDD[(K, V)]) {

    import RDD._
    //get 3 RDDs that shares key partitioning: leftExclusive, intersection, rightExclusive
    //all 3 can be zipped directly as if joined by key, this has many applications like getting union, intersection and subtraction
    //    def logicalCombinationsByKey[S](
    //                                     other: RDD[(K, V)])(
    //                                     innerReducer: (V, V) => V)(
    //                                     staging: RDD[(K, (Option[T], Option[T]))] => S
    //                                     ): (RDD[(K, V)], RDD[(K, (V, V))], RDD[(K, V)], S) = {
    //
    //      val cogrouped = self.cogroup(other)
    //
    //      val mixed: RDD[(K, (Option[T], Option[T]))] = cogrouped.mapValues{
    //        tuple =>
    //          val leftOption = tuple._1.reduceLeftOption(innerReducer)
    //          val rightOption = tuple._2.reduceLeftOption(innerReducer)
    //
    //          (leftOption, rightOption)
    //      }
    //      val stage = staging(mixed)
    //
    //      val leftExclusive = mixed.flatMapValues {
    //        case (Some(left), None) => Some(left)
    //        case _ => None
    //      }
    //      val Intersection = mixed.flatMapValues {
    //        case (Some(left), Some(right)) => Some(left, right)
    //        case _ => None
    //      }
    //      val rightExclusive = mixed.flatMapValues {
    //        case (None, Some(right)) => Some(right)
    //        case _ => None
    //      }
    //      (leftExclusive, Intersection, rightExclusive, stage)
    //    }

    def unionByKey(other: RDD[(K, V)])(
        innerReducer: (V, V) => V
    ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.mapValues { tuple =>
        val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
        reduced
      }
    }

    def intersectionByKey(other: RDD[(K, V)])(
        innerReducer: (V, V) => V
    ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.flatMap { triplet =>
        val tuple = triplet._2
        if (tuple._1.nonEmpty || tuple._2.nonEmpty) {
          val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
          Some(triplet._1 -> reduced)
        } else {
          None
        }
      }
    }

    //    /**
    //      * a beast of many form
    //      * @param other
    //      * @tparam V2
    //      * @return
    //      */
    //    def genJoin[V2: ClassTag](
    //                               other: RDD[(K, V2)],
    //                               cogroup: RDD[(K, V2)] => RDD[(K, (Iterable[T], Iterable[V2]))] = {
    //                                 v =>
    //                                   this.broadcastCogroup[V2](v)
    //                               }
    //                             ): RDD[(K, (V, V2))] = {
    //      cogroup(other).flatMap {
    //        case (k, (itr1, itr2)) =>
    //          val result = itr1.flatMap {
    //            v1 =>
    //              itr2.map {
    //                v2 =>
    //                  k -> (v1 -> v2)
    //              }
    //          }
    //          result
    //      }
    //    }
  }
  implicit class MapView[K, V](self: scala.collection.Map[K, V]) {

    assert(self != null)

    def getTyped[T: ClassTag](key: K): Option[T] = self.get(key) match {

      case Some(res) =>
        res match {
          case r: T => Some(r)
          case _    => None
        }
      case _ => None
    }

    def flattenByKey(
        key: K,
        sampler: Sampler[Any]
    ): Seq[(Map[K, Any], Int)] = {

      val valueOption: Option[V] = self.get(key)

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(SpookyUtils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val cleaned = self - key
      val result = sampled.toSeq.map(
        tuple => (cleaned + (key -> tuple._1)) -> tuple._2
      )

      result
    }

    def canonizeKeysToColumnNames: scala.collection.Map[String, V] = self.map(
      tuple => {
        val keyName: String = tuple._1 match {
          case symbol: scala.Symbol =>
            symbol.name //TODO: remove, this feature should no longer work after dataframe integration
          case _ =>
            tuple._1.toString
        }
        (SpookyUtils.canonizeColumnName(keyName), tuple._2)
      }
    )

    def sortBy[B: Ordering](fn: ((K, V)) => B): ListMap[K, V] = {
      val tuples = self.toList.sortBy(fn)
      ListMap(tuples: _*)
    }
  }

  implicit class TraversableLikeView[A, Repr](self: TraversableLike[A, Repr])(implicit ctg: ClassTag[A]) {

    def filterByType[B: ClassTag]: FilterByType[B] = new FilterByType[B]

    class FilterByType[B: ClassTag] {

      def get[That](implicit bf: CanBuildFrom[Repr, B, That]): That = {
        self.flatMap { v =>
          SpookyUtils.typedOrNone[B](v)
        }(bf)

        //        self.collect {case v: B => v} //TODO: switch to this after stop 2.10 support
      }
    }

    def mapToRDD[B: ClassTag](sc: SparkContext, local: Boolean = false, sliceOpt: Option[Int] = None)(
        f: A => B
    ): RDD[B] = {
      if (local) {
        sc.parallelize(
          self.toSeq.map(
            f
          ),
          sliceOpt.getOrElse(sc.defaultParallelism)
        )
      } else {
        sc.parallelize(
            self.toSeq,
            sliceOpt.getOrElse(sc.defaultParallelism)
          )
          .map(
            f
          )
      }
    }
  }

  implicit class ArrayView[A](self: Array[A]) {

    def filterByType[B <: A: ClassTag]: Array[B] = {
      self.flatMap { v =>
        SpookyUtils.typedOrNone[B](v)
      }

      //      self.collect {case v: B => v} //TODO: switch to this after stop 2.10 support
    }

    def flattenByIndex(
        i: Int,
        sampler: Sampler[Any]
    ): Seq[(Array[Any], Int)] = {

      val valueOption: Option[A] =
        if (self.indices contains i) Some(self.apply(i))
        else None

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(SpookyUtils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val result: Seq[(Array[Any], Int)] = sampled.toSeq.map { tuple =>
        val updated = self.updated(i, tuple._1)
        updated -> tuple._2
      }

      result
    }
  }

  implicit class DataFrameView(val self: DataFrame) {

    def toMapRDD(keepNull: Boolean = false): RDD[Map[String, Any]] = {
      val headers = self.schema.fieldNames

      val result: RDD[Map[String, Any]] = self.rdd.map { row =>
        ListMap(headers.zip(row.toSeq): _*)
      }

      val filtered =
        if (keepNull) result
        else
          result.map { map =>
            map.filter(_._2 != null)
          }

      filtered
    }
  }

  //  implicit class TraversableOnceView[A, Coll[A] <: TraversableOnce[A], Raw](self: Raw)(implicit cast: Raw => Coll[A]) {
  //
  //    def filterByType[B: ClassTag]: Coll[B] = {
  //      val result = cast(self).flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result.to[Coll[B]]
  //    }
  //  }

  //  implicit class ArrayView[A](self: Array[A]) {
  //
  //    def filterByType[B <: A: ClassTag]: Array[B] = {
  //      val result: Array[B] = self.flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result
  //    }
  //  }

  //  implicit class TraversableLikeView[+A, +Repr, Raw](self: Raw)(implicit cast: Raw => TraversableLike[A,Repr]) {
  //
  //    def filterByType[B] = {
  //      val v = cast(self)
  //      val result = v.flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result
  //    }
  //  }
}

object SpookyViews extends SpookyViews {}
