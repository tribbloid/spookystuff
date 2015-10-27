package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.actions.DryRun
import com.tribbloids.spookystuff.entity.PageRow._
import com.tribbloids.spookystuff.entity.{PageRow, Squashed}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by peng on 11/7/14.
 * implicit conversions in this package are used for development only
 */
object Views {

  val SPARK_JOB_DESCRIPTION = "spark.job.description"
  val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

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

  //  implicit class RDDView[A](val self: A)(implicit ev1: A => RDD[_]) {

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
  //  }

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
  }

  implicit class WebCacheRDDView(self: WebCacheRDD) {

    import RDD._

    def discardRows: WebCacheRDD = {

      self.mapValues {
        _.copy(rows = Array())
      }
    }

    def getRows: RDD[PageRow] = {

      val updated = self.flatMap {
        _._2.rows
      }

      updated
    }

    /*
     this add rows into WebCacheRDD using the following rules:
     WebCacheRDD use dryrun as the only index,
     any PageRow RDD with S having the same dryrun sequence will be cogrouped into the same WebCacheRow
     each WebCacheRow must have a unique dryrun, otherwise its defective
     if >1 new PageRow's dryrun doesn't exist in the WebCacheRow,
     they are deduplicated to ensure that 2 rows in the same segment won't have the identical pages
     if the dryrun already exists,
     rows in the same segment with identical pages are removed first.
     Then deduplicated.
    */
    def putRows(
                 rows: RDD[PageRow],
                 seedFilter: Iterable[PageRow] => Option[PageRow] = v => v.headOption,
                 partitionerOption: Option[Partitioner] = None
                 ): WebCacheRDD = {

      val dryRun_RowRDD = rows.keyBy(_.dryrun)
      val cogrouped = partitionerOption match {
        case Some(partitioner) => self.cogroup(dryRun_RowRDD, partitioner)
        case None => self.cogroup(dryRun_RowRDD)
      }
      val result = cogrouped.map {
        (triplet: (DryRun, (Iterable[Squashed[PageRow]], Iterable[PageRow]))) =>
          val tuple: (Iterable[Squashed[PageRow]], Iterable[PageRow]) = triplet._2
          //          assert(tuple._1.size <= 1)
          val squashedRowOption = tuple._1.headOption //can only be 0 or 1
        val newRows = tuple._2

          squashedRowOption match {
            case None =>

              val seedRows = newRows
                .groupBy(_.uid)
                .flatMap(tuple => seedFilter(tuple._2))

              val newPageLikes = seedRows.head.pageLikes //all newRows should have identical pageLikes (if using wide optimizer)

              val newCached = triplet._1 -> Squashed(pageLikes = newPageLikes, rows = seedRows.toArray)
              newCached
            case Some(squashedRow) =>

              val existingRowUIDs: Array[RowUID] = squashedRow.rows.map(_.uid)
              val seedRows = newRows
                .filterNot(row => existingRowUIDs.contains(row.uid))
                .groupBy(_.uid)
                .flatMap(tuple => seedFilter(tuple._2))

              val newCached = triplet._1 -> squashedRow.copy(rows = squashedRow.rows ++ seedRows)
              newCached
          }
      }
      result
    }
  }

  implicit class MapView[K, V](m1: Map[K,V]) {

    def getTyped[T: ClassTag](key: K): Option[T] = m1.get(key) match {

      case Some(res) =>
        res match {
          case r: T => Some(r)
          case _ => None
        }
      case _ => None
    }

    //  def merge(m2: Map[K,_], strategy: MergeStrategy): Map[K,_] = {
    //    val halfMerged = m2.map{
    //      kv => {
    //        m1.get(kv._1) match {
    //          case None => kv
    //          case Some(v) => (kv._1, strategy.f(v, kv._2))
    //        }
    //      }
    //    }
    //
    //    m1 ++ halfMerged
    //  }

    def flattenKey(
                    key: K
                    ): Seq[Map[K,_]] = {

      val valueOption = m1.get(key)

      val values: Iterable[_] = valueOption.toSeq.flatMap(Utils.encapsulateAsIterable)

      val cleaned = m1 - key
      val result = values.toSeq.map(value => cleaned + (key-> value))

      result
    }

    def canonizeKeysToColumnNames: Map[String,V] = m1.map(
      tuple =>{
        val keyName = tuple._1 match {
          case symbol: Symbol =>
            symbol.name
          case _ =>
            tuple._1.toString
        }
        (Utils.canonizeColumnName(keyName), tuple._2)
      }
    )
  }

}
