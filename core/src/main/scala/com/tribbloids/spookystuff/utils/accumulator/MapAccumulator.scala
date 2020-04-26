package com.tribbloids.spookystuff.utils.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

case class MapAccumulator[K, V](
    override val value: mutable.LinkedHashMap[K, V] = mutable.LinkedHashMap.empty[K, V],
    updateIfExists: Boolean = false
) extends AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]] {

  override def isZero: Boolean = value.isEmpty

  override def copy(): AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]] = {
    val neo = MapAccumulator[K, V]()
    neo.value ++= this.value
    neo
  }

  override def reset(): Unit = {
    value.clear()
  }

  override def add(v: (K, V)): Unit = {

    if (updateIfExists)
      value.update(v._1, v._2)
    else
      value.getOrElseUpdate(v._1, v._2)
  }

  override def merge(other: AccumulatorV2[(K, V), mutable.LinkedHashMap[K, V]]): Unit = {
    for (v <- other.value) {
      add(v)
    }
  }
}
