package com.tribbloids.spookystuff.utils

import ai.acyclic.prover.commons.spark.SparkContextView
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 11/7/14. implicit conversions in this package are used for development only
  */
object RDDImplicits extends RDDImplicits

trait RDDImplicits extends SpookyViews_Imp0 with ai.acyclic.prover.commons.spark.RDDImplicits {

  implicit def sparkContextView(self: SparkContext): SparkContextView = SparkContextView(self)

  implicit class StringRDDView(val self: RDD[String]) {

    // csv has to be headerless, there is no better solution as header will be shuffled to nowhere
    def csvToMap(headerRow: String, splitter: String = ","): RDD[Map[String, String]] = {
      val headers = headerRow.split(splitter)

      // cannot handle when a row is identical to headerline, but whatever
      self.map { str =>
        {
          val values = str.split(splitter)

          ListMap(headers.zip(values) *)
        }
      }
    }

    def tsvToMap(headerRow: String): RDD[Map[String, String]] = csvToMap(headerRow, "\t")
  }

  implicit class PairRDDView[K: ClassTag, V: ClassTag](val self: RDD[(K, V)]) {

    import RDD.*
    // get 3 RDDs that shares key partitioning: leftExclusive, intersection, rightExclusive
    // all 3 can be zipped directly as if joined by key, this has many applications like getting union, intersection and subtraction
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
}
