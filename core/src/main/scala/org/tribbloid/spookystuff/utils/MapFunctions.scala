package org.tribbloid.spookystuff.utils

import scala.reflect.ClassTag

/**
 * Created by peng on 10/29/14.
 */
class MapFunctions[K](m1: Map[K,_]) {

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

  def keyFlatten(
                  key: K,
                  indexKey: K,
                  idempotent: Boolean
                  ): Seq[Map[K,_]]  = keyFlatten(key,key,indexKey,idempotent)

  def keyFlatten(
                  oldKey: K,
                  newKey: K,
                  indexKey: K,
                  idempotent: Boolean
                  ): Seq[Map[K,_]] = {

    val valueOption = m1.get(oldKey)

    val value: Iterable[_] = valueOption.seq.flatMap {
      case v: TraversableOnce[_] => v
      case v: Any =>
        if (idempotent) Seq[Any](v)
        else throw new UnsupportedOperationException("cannot flatten a value that is not traversable")
    }

    val cleaned = if (oldKey != newKey) m1 - oldKey
    else m1
    val result = Option(indexKey) match {
      case Some(str) => value.zipWithIndex.toSeq.map(value => cleaned + (newKey-> value._1, indexKey -> value._2))
      case None => value.toSeq.map(value => cleaned + (newKey-> value))
    }
    if (result.isEmpty) result :+ Seq(cleaned)
    result
  }
}