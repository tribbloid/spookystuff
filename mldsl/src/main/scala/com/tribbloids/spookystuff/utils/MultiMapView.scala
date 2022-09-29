package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.MultiMapView.Self

import scala.collection.mutable
import scala.language.{higherKinds, implicitConversions}

trait MultiMapView[K, +V] {

  def self: Self[K, V]

  protected def mergeProto[V2 >: V](other: MultiMapView[K, V2]): MultiMapView.Mutable[K, V2] = {

    val buffer: MultiMapView.Mutable[K, V2] = mutable.HashMap.empty[K, Seq[V2]]

    val operands: Seq[collection.Map[K, Seq[V2]]] = Seq(this.self, other.self)

    for (
      map <- operands;
      (k, vs) <- map
    ) {

      buffer.putN(k, vs)
    }

    buffer
  }

  def merge[V2 >: V](other: MultiMapView[K, V2]): MultiMapView.Mutable[K, V2] = {

    val buffer = mergeProto(other)
    buffer
  }

  def +:+[V2 >: V](other: MultiMapView[K, V2]): MultiMapView.Mutable[K, V2] = merge(other)
}

/**
  * not thread safe
  */
object MultiMapView {

  type Self[K, +V] = collection.Map[K, Seq[V]]
  type MSelf[K, V] = mutable.Map[K, Seq[V]]

  implicit def fromSelf[K, V](self: Self[K, V]): Immutable[K, V] = new Immutable(self)
  implicit def toSelf[K, V](v: MultiMapView[K, V]): Self[K, V] = v.self

  implicit def fromMSelf[K, V](self: MSelf[K, V]): Mutable[K, V] = new Mutable(self)
  implicit def toMSelf[K, V](v: Mutable[K, V]): MSelf[K, V] = v.self

  class Immutable[K, V](override val self: Self[K, V]) extends MultiMapView[K, V] {}

  class Mutable[K, V](override val self: MSelf[K, V]) extends Immutable[K, V](self) with MultiMapView[K, V] {

    def put1(k: K, v: V): Unit = {

      putN(k, Seq(v))
    }

    def putN(k: K, vs: Seq[V]): Unit = {

      val existing = self.getOrElse(k, Nil)
      self += (k -> (existing ++ vs))
    }

    def distinctValues(k: K): Unit = {
      val existingOpt = self.get(k)
      existingOpt.foreach { existing =>
        self += (k -> existing.distinct)
      }
    }

    def distinctAllValues(): Unit = {

      self.keys.foreach { k =>
        distinctValues(k)
      }
    }

    def filterValue(k: K)(condition: V => Boolean): Option[Seq[V]] = {
      self.get(k).map { seq =>
        val filtered = seq.filter(condition)
        self += k -> filtered
        if (filtered.isEmpty)
          self -= k

        filtered
      }
    }
  }

  trait Factory {

    type View[K, V]

    def apply[K, V](kvs: (K, V)*): View[K, V]

    final def empty[K, V]: View[K, V] = apply()
  }

  object Immutable extends Factory {

    type View[K, V] = Immutable[K, V]

    override def apply[K, V](kvs: (K, V)*): Immutable[K, V] = {
      val buffer: Mutable[K, V] = Mutable(kvs: _*)

      new Immutable(buffer.self)
    }
  }

  object Mutable extends Factory {

    type View[K, V] = Mutable[K, V]

    def apply[K, V](kvs: (K, V)*): Mutable[K, V] = {
      val buffer: Mutable[K, V] = mutable.HashMap.empty[K, Seq[V]]

      kvs.foreach {
        case (k, v) => buffer.put1(k, v)
      }

      buffer
    }
  }

//  object Mutable_List extends Factory {
//
//    type View[K, V] = Mutable[K, V]
//
//    def apply[K, V](kvs: (K, V)*): Mutable[K, V] = {
//      val buffer: Mutable[K, V] = mutable.ListMap.empty[K, Seq[V]]
//
//      kvs.foreach {
//        case (k, v) => buffer.put1(k, v)
//      }
//
//      buffer
//    }
//  }
}
