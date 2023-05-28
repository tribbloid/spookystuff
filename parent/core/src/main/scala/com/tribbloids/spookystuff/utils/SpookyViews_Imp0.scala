package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.row._

import scala.collection.immutable.ListMap
import scala.collection.{Factory, Map}
import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class SpookyViews_Imp0 extends CommonViews {

  implicit class TraverseOps[F[t] <: Iterable[t], A](self: F[A]) {

    def filterByType[B <: A: ClassTag](
        implicit
        toF: Factory[B, F[B]]
    ): F[B] = {

      val itr = self.iterator.flatMap { v =>
        val result = SpookyUtils.typedOrNone[B](v)
        result
      }

      toF.fromSpecific(itr)
    }
  }

  implicit def arrayOps[A](self: Array[A]): TraverseOps[Seq, A] = new TraverseOps(self.toList)

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
      val result = sampled.toSeq.map(tuple => (cleaned + (key -> tuple._1)) -> tuple._2)

      result
    }

    def canonizeKeysToColumnNames: scala.collection.Map[String, V] = self.map(tuple => {
      val keyName: String = tuple._1 match {
        case symbol: scala.Symbol =>
          symbol.name // TODO: remove, this feature should no longer work after dataframe integration
        case _ =>
          tuple._1.toString
      }
      (SpookyUtils.canonizeColumnName(keyName), tuple._2)
    })

    def sortBy[B: Ordering](fn: ((K, V)) => B): ListMap[K, V] = {
      val tuples = self.toList.sortBy(fn)
      ListMap(tuples: _*)
    }
  }
}
