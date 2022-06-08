package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.row._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.ListMap
import scala.collection.{Map, TraversableLike}
import scala.reflect.ClassTag

abstract class SpookyViews_Imp0 extends CommonViews {

  //TODO: the following 2 can be merged into 1
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
}
