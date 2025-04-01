package com.tribbloids.spookystuff.utils.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

// TODO: INCOMPLETE! this implementation always replace old KV instead of updating it.
//  should add an extra layer of abstraction
case class MapAccumulator[K, V](
    map: mutable.LinkedHashMap[K, V] = mutable.LinkedHashMap.empty[K, V],
    updater: (V, V) => V = MapAccumulator.Updater.Replace[V]()
) extends AccumulatorV2[(K, V), MapAccumulator.MapViz[K, V]] {

  import MapAccumulator.*

  override def value: MapViz[K, V] = MapViz(map)

  override def isZero: Boolean = map.isEmpty

  override def copy(): MapAccumulator[K, V] = {
    val neo = MapAccumulator[K, V](updater = updater)
    neo.map ++= this.map
    neo
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: (K, V)): Unit = {

    val oldVOpt = map.get(v._1)

    val newV = oldVOpt match {
      case Some(oldV) => updater(oldV, v._2)
      case None       => v._2
    }
    map.update(v._1, newV)
  }

  override def merge(other: AccumulatorV2[(K, V), MapViz[K, V]]): Unit = {
    for (v <- other.value.self) {
      add(v)
    }
  }
}

object MapAccumulator {

  case class MapViz[K, V](
      self: collection.Map[K, V]
  ) {

    override lazy val toString: String = {

      self
        .map {
          case (k, v) => s"$k -> $v"
        }
        .mkString(", ")
    }
  }

  type Updater[V] = (V, V) => V

  object Updater {

    case class Replace[V]() extends Updater[V] {
      override def apply(v1: V, v2: V): V = v1
    }

    case class KeepOld[V]() extends Updater[V] {
      override def apply(v1: V, v2: V): V = v2
    }
  }

  def kToLong[K]: MapAccumulator[K, Long] = MapAccumulator[K, Long](updater = _ + _)
}
