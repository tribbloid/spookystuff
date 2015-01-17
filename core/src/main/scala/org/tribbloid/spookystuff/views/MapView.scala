package org.tribbloid.spookystuff.views

import org.tribbloid.spookystuff.utils.Utils

import scala.reflect.ClassTag

/**
 * Created by peng on 10/29/14.
 */
class MapView[K, V](m1: Map[K,V]) {

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
                  key: K,
                  indexKey: K
                  ): Seq[Map[K,_]] = {

    val valueOption = m1.get(key)

    val values: Iterable[_] = valueOption.toSeq.flatMap(Utils.encapsulateAsIterable)

    val cleaned = m1 - key
    val result = Option(indexKey) match {
      case Some(str) => values.zipWithIndex.toSeq.map(value => cleaned + (key-> value._1, indexKey -> value._2))
      case None => values.toSeq.map(value => cleaned + (key-> value))
    }

    result
  }

  def canonizeKeysToColumnNames: Map[String,V] = m1.map(
    tuple =>
      (Utils.canonizeColumnName(tuple._1.toString), tuple._2)
  )
}