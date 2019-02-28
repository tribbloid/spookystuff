package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.OrderedMultiMapView.Self

import scala.collection.mutable
import scala.language.implicitConversions

trait OrderedMultiMapView[K, +V] {

  def self: Self[K, V]

  protected def mergeProto[V2 >: V](other: OrderedMultiMapView[K, V2]): OrderedMultiMapView.Mutable[K, V2] = {

    val buffer: OrderedMultiMapView.Mutable[K, V2] = mutable.HashMap.empty[K, Seq[V2]]

    val operands: Seq[collection.Map[K, Seq[V2]]] = Seq(this.self, other.self)

    for (map <- operands;
         (k, v) <- map) {

      buffer.putN(k, v)
    }

    buffer
  }

  def merge[V2 >: V](other: OrderedMultiMapView[K, V2]): OrderedMultiMapView.Mutable[K, V2] = {

    val buffer = mergeProto(other)
    buffer
  }

  def +:+[V2 >: V](other: OrderedMultiMapView[K, V2]): OrderedMultiMapView.Mutable[K, V2] = merge(other)
}

/**
  * not thread safe
  */
object OrderedMultiMapView {

  type Self[K, V] = collection.Map[K, Seq[V]]
  type MSelf[K, V] = mutable.Map[K, Seq[V]]

  class Immutable[K, V](override val self: Self[K, V]) extends OrderedMultiMapView[K, V] {}

  class Mutable[K, V](override val self: MSelf[K, V]) extends Immutable[K, V](self) {

    def put1(k: K, v: V): Unit = {

      putN(k, Seq(v))
    }

    def putN(k: K, vs: Seq[V]): Unit = {

      val existing = self.getOrElse(k, Nil)
      self += (k -> (existing ++ vs))
    }

    def filterValue(k: K)(condition: V => Boolean): Option[Seq[V]] = {
      self.get(k).map { seq =>
        val filtered = seq.filter(condition)
        self += k -> filtered
        filtered
      }
    }
  }

  trait Implicits {

    implicit def fromSelf[K, V](self: Self[K, V]): Immutable[K, V] = new Immutable(self)

    implicit def toSelf[K, V](v: OrderedMultiMapView[K, V]): Self[K, V] = v.self
  }

  object Immutable extends Implicits {
    def apply[K, V](kvs: (K, V)*): Immutable[K, V] = {
      val buffer: Mutable[K, V] = Mutable(kvs: _*)

      new Immutable(buffer.self)
    }

    private lazy val emptyProto: Immutable[Nothing, Nothing] = Immutable()

    def empty[K, V]: Immutable[K, V] = emptyProto.asInstanceOf[Immutable[K, V]]
  }

  object Mutable extends Implicits {

    implicit def fromMSelf[K, V](self: MSelf[K, V]): Mutable[K, V] = new Mutable(self)

    implicit def toMSelf[K, V](v: Mutable[K, V]): MSelf[K, V] = v.self

    def apply[K, V](kvs: (K, V)*): Mutable[K, V] = {
      val buffer: Mutable[K, V] = mutable.HashMap.empty[K, Seq[V]]

      kvs.foreach {
        case (k, v) => buffer.put1(k, v)
      }

      buffer
    }

    def empty[K, V]: Mutable[K, V] = Mutable()
  }
}
