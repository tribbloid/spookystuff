package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.row._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.generic.CanBuildFrom
import scala.collection.{Map, TraversableLike}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
  * Created by peng on 11/7/14.
  * implicit conversions in this package are used for development only
  */
object Implicits {

  val SPARK_JOB_DESCRIPTION = "spark.job.description"
  val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  implicit def pageRowToView(self: PageRow): PageRowView = PageRowView(self)

  implicit class SparkContextView(val self: SparkContext) {

    def withJob[T](description: String)(fn: T): T = {

      val oldDescription = self.getLocalProperty(SPARK_JOB_DESCRIPTION)
      if (oldDescription == null) self.setJobDescription(description)
      else self.setJobDescription(oldDescription + " > " + description)

      val result: T = fn
      self.setJobGroup(null,oldDescription)
      result
    }
  }

  implicit class RDDView[T](val self: RDD[T]) {

    def named = {
      val stackTraceElements: Array[StackTraceElement] = Thread.currentThread().getStackTrace
      stackTraceElements
    }

    def collectPerPartition: Array[List[T]] = self.mapPartitions(v => Iterator(v.toList)).collect()

    def multiPassMap[U: ClassTag](f: T => Option[U]): RDD[U] = {

      multiPassFlatMap(f.andThen(v => v.map(Traversable(_))))
    }

    //if the function returns None for it will be retried as many times as it takes to get rid of them.
    //core problem is optimization: how to SPILL properly and efficiently?
    //TODO: this is the first implementation, simple but may not the most efficient
    def multiPassFlatMap[U: ClassTag](f: T => Option[TraversableOnce[U]]): RDD[U] = {

      val counter = self.sparkContext.accumulator(0, "unprocessed data")
      var halfDone: RDD[Either[T, TraversableOnce[U]]] = self.map(v => Left(v))

      while(true) {
        counter.setValue(0)

        val updated: RDD[Either[T, TraversableOnce[U]]] = halfDone.map {
          case Left(src) =>
            f(src) match {
              case Some(res) => Right(res)
              case None =>
                counter += 1
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

    //  def checkpointNow(): Unit = {
    //    persistDuring(StorageLevel.MEMORY_ONLY) {
    //      self.checkpoint()
    //      self.foreach(_ =>)
    //      self
    //    }
    //    Unit
    //  }
  }

  implicit class PairRDDView[K: ClassTag, V: ClassTag](val self: RDD[(K, V)]) {

    import RDD._
    //get 3 RDDs that shares key partitioning: leftExclusive, intersection, rightExclusive
    //all 3 can be zipped directly as if joined by key, this has many applications like getting union, intersection and subtraction
    //    def logicalCombinationsByKey[S](
    //                                     other: RDD[(K, V)])(
    //                                     innerReducer: (V, V) => V)(
    //                                     staging: RDD[(K, (Option[V], Option[V]))] => S
    //                                     ): (RDD[(K, V)], RDD[(K, (V, V))], RDD[(K, V)], S) = {
    //
    //      val cogrouped = self.cogroup(other)
    //
    //      val mixed: RDD[(K, (Option[V], Option[V]))] = cogrouped.mapValues{
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

    def unionByKey(
                    other: RDD[(K, V)])(
                    innerReducer: (V, V) => V
                  ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.mapValues {
        tuple =>
          val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
          reduced
      }
    }

    def intersectionByKey(
                           other: RDD[(K, V)])(
                           innerReducer: (V, V) => V
                         ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.flatMap {
        triplet =>
          val tuple = triplet._2
          if (tuple._1.nonEmpty || tuple._2.nonEmpty) {
            val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
            Some(triplet._1 -> reduced)
          }
          else {
            None
          }
      }
    }

    def groupByKey_narrow(): RDD[(K, Iterable[V])] = {

      self.mapPartitions{
        itr =>
          itr
            .toTraversable
            .groupBy(_._1)
            .map(v => v._1 -> v._2.map(_._2).toIterable)
            .iterator
      }
    }
    def reduceByKey_narrow(
                            reducer: (V, V) => V
                          ): RDD[(K, V)] = {
      self.mapPartitions{
        itr =>
          itr
            .toTraversable
            .groupBy(_._1)
            .map(v => v._1 -> v._2.map(_._2).reduce(reducer))
            .iterator
      }
    }

    def groupByKey_beacon[T](
                              beaconRDD: RDD[(K, T)]
                            ): RDD[(K, Iterable[V])] = {

      val cogrouped = self.cogroup(beaconRDD, beaconRDD.partitioner.get)
      cogrouped.mapValues {
        tuple =>
          tuple._1
      }
    }

    def reduceByKey_beacon[T](
                               reducer: (V, V) => V,
                               beaconRDD: RDD[(K, T)]
                             ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(beaconRDD, beaconRDD.partitioner.get)
      cogrouped.mapValues {
        tuple =>
          tuple._1.reduce(reducer)
      }
    }
  }

  implicit class MapView[K, V](m1: scala.collection.Map[K,V]) {

    def getTyped[T: ClassTag](key: K): Option[T] = m1.get(key) match {

      case Some(res) =>
        res match {
          case r: T => Some(r)
          case _ => None
        }
      case _ => None
    }

    def flattenByKey(
                      key: K,
                      sampler: Sampler[Any]
                    ): Seq[(Map[K, Any], Int)] = {

      val valueOption: Option[V] = m1.get(key)

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(Utils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val cleaned = m1 - key
      val result = sampled.toSeq.map(
        tuple =>
          (cleaned + (key -> tuple._1)) -> tuple._2
      )

      result
    }

    def canonizeKeysToColumnNames: scala.collection.Map[String,V] = m1.map(
      tuple =>{
        val keyName: String = tuple._1 match {
          case symbol: scala.Symbol =>
            symbol.name
          case _ =>
            tuple._1.toString
        }
        (Utils.canonizeColumnName(keyName), tuple._2)
      }
    )
  }

  implicit class TraversableLikeView[A, Repr](self: TraversableLike[A, Repr]) {

    def filterByType[B: ClassTag] = new FilterByType[B]

    class FilterByType[B: ClassTag] {

      def get[That](implicit bf: CanBuildFrom[Repr, B, That]): That = {
        val result = self.flatMap{
          v =>
            Utils.typedOrNone[B](v)
        }(bf)
        result
      }
    }
  }

  implicit class ArrayView[A](self: Array[A]) {

    def filterByType[B <: A: ClassTag]: Array[B] = {
      self.flatMap{
        v =>
          Utils.typedOrNone[B](v)
      }.toArray
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
