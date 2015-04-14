package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.utils.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 11/7/14.
 */
//implicit conversions in this package are used for development only
package object views {

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