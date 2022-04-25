package com.tribbloids.spookystuff.utils.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

// TODO: INCOMPLETE! this implementation always replace old KV instead of updating it.
//  should add an extra layer of abstraction
case class MapAccumulator[K, V](
    override val value: mutable.LinkedHashMap[K, V] = mutable.LinkedHashMap.empty[K, V],
    updater: (V, V) => V = MapAccumulator.Updater.Replace[V]()
) extends AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]] {

  override def isZero: Boolean = value.isEmpty

  override def copy(): AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]] = {
    val neo = MapAccumulator[K, V](updater = updater)
    neo.value ++= this.value
    neo
  }

  override def reset(): Unit = {
    value.clear()
  }

  override def add(v: (K, V)): Unit = {

    val oldVOpt = value.get(v._1)

    val newV = oldVOpt match {
      case Some(oldV) => updater(oldV, v._2)
      case None       => v._2
    }
    value.update(v._1, newV)
  }

  override def merge(other: AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]]): Unit = {
    for (v <- other.value) {
      add(v)
    }
  }
}

object MapAccumulator {

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
