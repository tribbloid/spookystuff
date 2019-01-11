package com.tribbloids.spookystuff.metrics

import java.lang

import com.tribbloids.spookystuff.metrics.Metrics.Acc
import org.apache.spark.SparkContext
import org.apache.spark.ml.dsl.utils.NestedMap
import org.apache.spark.ml.dsl.utils.messaging.ProtoAPI
import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils
import org.apache.spark.sql.execution.streaming.EventTimeStatsAccum
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

import scala.language.implicitConversions

/**
  * Created by peng on 03/10/15.
  */
object Metrics {

  trait CanBuildFrom[T1, IN <: AccumulatorV2[_, _]] extends (T1 => Acc[IN])
  trait BuildLong[T] extends CanBuildFrom[T, LongAccumulator] {
    override def apply(v1: T) = new Acc(new LongAccumulator()) {
      override def +=(v: Number): Unit = {
        val vv = v.longValue()
        self.add(vv)
      }
    }
  }
  trait BuildDouble[T] extends CanBuildFrom[T, DoubleAccumulator] {
    override def apply(v1: T) = new Acc(new DoubleAccumulator()) {
      override def +=(v: Number): Unit = {
        val vv = v.doubleValue()
        self.add(vv)
      }
    }
  }

  trait BuildTimeStats[T] extends CanBuildFrom[T, EventTimeStatsAccum] {
    override def apply(v1: T): Acc[EventTimeStatsAccum] = new Acc(new EventTimeStatsAccum()) {
      override def +=(v: Number): Unit = {
        val vv = v.longValue()
        self.add(vv)
      }

      override def normalized: Any = self.value.count
    }
  }
  abstract class CanBuildFromLevel1 {

    implicit object Long2TimeStats extends BuildTimeStats[Long]
    implicit object JLong2TimeStats extends BuildTimeStats[lang.Long]
    implicit object Int2TimeStats extends BuildTimeStats[Int]
  }

  object CanBuildFrom extends CanBuildFromLevel1 {

    implicit object Long2Long extends BuildLong[Long]
    implicit object JLong2Long extends BuildLong[lang.Long]
    implicit object Int2Long extends BuildLong[Int]

    implicit object Double2Double extends BuildDouble[Double]
    implicit object JDouble2Double extends BuildDouble[lang.Double]
    implicit object Float2Double extends BuildDouble[Float]
  }

  //TODO: is this efficient?
  abstract class Acc[T <: AccumulatorV2[_, _]](
      val self: T
  ) extends Serializable {

    def +=(v: Number): Unit //adapter that does type cast

    def reset(): Unit = self.reset()
    def name = self.name

    def normalized: Any = self.value
  }

  object Acc {

    implicit def fromV[T1, SS <: AccumulatorV2[_, _]](
        value: T1
    )(
        implicit
        canBuildFrom: CanBuildFrom[T1, SS],
        cast: T1 => Number,
        sc: SparkContext = SparkContext.getOrCreate()
    ): Acc[SS] = {
      Metrics.accumulator(value)
    }

    implicit def fromKV[T1, SS <: AccumulatorV2[_, _]](
        kv: (String, T1)
    )(
        implicit
        canBuildFrom: CanBuildFrom[T1, SS],
        cast: T1 => Number,
        sc: SparkContext = SparkContext.getOrCreate()
    ): Acc[SS] = {
      Metrics.accumulator(kv._2, kv._1)
    }

    implicit def toAccumV2[T <: AccumulatorV2[_, _]](v: Acc[T]): T = v.self
  }

  def accumulator[T1, SS <: AccumulatorV2[_, _]](
      value: T1,
      name: String = null
  )(
      implicit
      canBuildFrom: CanBuildFrom[T1, SS],
      cast: T1 => Number,
      sc: SparkContext = SparkContext.getOrCreate()
  ): Acc[SS] = {

    val acc: Acc[SS] = canBuildFrom(value)
    acc.reset()
    acc += value

    Option(name) match {
      case Some(nn) =>
        sc.register(acc.self, nn)
      case None =>
        sc.register(acc.self)
    }

    acc
  }
}

@SerialVersionUID(-32509237409L)
abstract class Metrics extends ProtoAPI with Product with Serializable {

  def name: Option[String] = None

  //Only allowed on Master
  def zero(): Unit = {

    this.productIterator.toList.foreach {
      case acc: Acc[_] =>
        acc.reset()
      case _ =>
    }
  }

  //this is necessary as direct JSON serialization on accumulator only yields meaningless string
  def toTuples[T](
      fn: Metrics.Acc[_ <: AccumulatorV2[_, _]] => Option[T]
  ): NestedMap[T] = {
    val result = NestedMap[T]()
    val caseAccessors = ReflectionUtils.getCaseAccessorMap(this)
    caseAccessors.foreach {
      case (_name: String, acc: Acc[_]) =>
        val name = acc.name.getOrElse(_name)
        fn(acc).foreach(v => result += name -> Left(v))

      case (_name: String, coll: Metrics) =>
        val name = coll.name.getOrElse(_name)
        result += name -> Right(coll.toTuples[T](fn))

      case _ =>
        None
    }
    result
  }

  def toNestedMap: NestedMap[Any] = {

    val result: NestedMap[Any] = toTuples(acc => Some(acc.value))
    result
  }

  def toMap: Map[String, Any] = toNestedMap.flattenLeaves

  //DO NOT change to val! metrics is very mutable
  override def toMessage_>> : NestedMap[Any] = toNestedMap

  object flattenLeaves extends ProtoAPI {
    override def toMessage_>> : Map[String, Any] = toMap
  }
}
