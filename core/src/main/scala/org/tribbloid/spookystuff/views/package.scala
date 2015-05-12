package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.tribbloid.spookystuff.entity.{Squashed, PageRow}
import org.tribbloid.spookystuff.entity.PageRow.WebCacheRDD
import org.tribbloid.spookystuff.utils.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 11/7/14.
 * implicit conversions in this package are used for development only
 */
package object views {

  implicit class RDDView[V](val self: RDD[V]) {

    def persistDuring[T](newLevel: StorageLevel, blocking: Boolean = true)(fn: => T): T =
      if (self.getStorageLevel == StorageLevel.NONE){
        self.persist(newLevel)
        val result = fn
        self.unpersist(blocking)
        result
      }
      else {
        val result = fn
        self.unpersist(blocking)
        result
      }

    //  def checkpointNow(): Unit = {
    //    persistDuring(StorageLevel.MEMORY_ONLY) {
    //      self.checkpoint()
    //      self.count()
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
  }

  implicit class InMemoryWebCacheRDDView(self: WebCacheRDD) {

    import RDD._

    def discardRows: WebCacheRDD = {

      self.mapValues {
        _.copy(metadata = Array())
      }
    }

    def getRows: RDD[PageRow] = {

      val updated = self.flatMap {
        _._2.metadata
      }

      updated
    }

    def indexRows(
                   rows: RDD[PageRow],
                   seedFilter: Iterable[PageRow] => Option[PageRow] = _=>None
                   ): WebCacheRDD = {

      val dryRun_RowRDD = rows.keyBy(_.dryrun)
      val cogrouped = self.cogroup(dryRun_RowRDD)
      val result = cogrouped.map {
        triplet =>
          val tuple = triplet._2
          assert(tuple._1.size <= 1)
          val squashedRowOption = triplet._2._1.reduceOption(_ ++ _)
          squashedRowOption match {
            case None =>
              val newRows = tuple._2

              val seedRows = seedFilter(newRows)
                .groupBy(_.uid)
                .flatMap(tuple => seedFilter(tuple._2))

              val newPageLikes = seedFilter(newRows).get.pageLikes

              val newCached = triplet._1 -> Squashed(newPageLikes, seedRows.toArray)
              newCached
            case Some(squashedRow) =>
              val newRows = tuple._2

              val existingRowUIDs = squashedRow.metadata.map(_.uid)
              val seedRows = newRows.filterNot(row => existingRowUIDs.contains(row.uid))
                .groupBy(_.uid)
                .flatMap(tuple => seedFilter(tuple._2))

              val newCached = triplet._1 -> squashedRow.copy(metadata = squashedRow.metadata ++ seedRows)
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
      tuple =>
        (Utils.canonizeColumnName(tuple._1.toString), tuple._2)
    )
  }

}