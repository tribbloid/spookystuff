package com.tribbloids.spookystuff.metrics

import java.lang

import com.tribbloids.spookystuff.metrics.Metrics.Acc
import org.apache.spark.SparkContext
import org.apache.spark.ml.dsl.utils.NestedMap
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
    override def apply(v1: T): Acc[LongAccumulator] = new Acc(new LongAccumulator()) {
      override def +=(v: Number): Unit = {
        val vv = v.longValue()
        self.add(vv)
      }
    }
  }
  trait BuildDouble[T] extends CanBuildFrom[T, DoubleAccumulator] {
    override def apply(v1: T): Acc[DoubleAccumulator] = new Acc(new DoubleAccumulator()) {
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
  /**
    *
    * @param self: AccumulatorV2
    */
  abstract class Acc[T <: AccumulatorV2[_, _]](
      val self: T
  ) extends Serializable {

    def +=(v: Number): Unit //adapter that does type cast

    def reset(): Unit = self.reset()
    def name: Option[String] = self.name
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

  /**
    * @param displayName: if undefined,
    *                           use member variable name obtained through [[ReflectionUtils.getCaseAccessorMap]]
    */
  def accumulator[T1, SS <: AccumulatorV2[_, _]](
      value: T1,
      displayName: String = null
  )(
      implicit
      canBuildFrom: CanBuildFrom[T1, SS],
      cast: T1 => Number,
      sc: SparkContext = SparkContext.getOrCreate()
  ): Acc[SS] = {

    val acc: Acc[SS] = canBuildFrom(value)
    acc.reset()
    acc += value

    Option(displayName) match {
      case Some(nn) =>
        sc.register(acc.self, nn)
      case None =>
        sc.register(acc.self)
    }
    acc
  }
}

@SerialVersionUID(-32509237409L)
abstract class Metrics extends Product with Serializable {

//  def sparkContext: SparkContext = SparkContext.getOrCreate()

  @transient lazy val caseAccessorMapProto: List[(String, Any)] = ReflectionUtils.getCaseAccessorMap(this)

  /**
    * slow, should not be used too often
    */
  def getCaseAccessorMap(useDisplayName: Boolean = true): List[(String, Any)] = {

    if (!useDisplayName) caseAccessorMapProto
    else {
      caseAccessorMapProto.map {
        case (k, v: Acc[_]) =>
          val name = v.self.name.getOrElse(k)
          name -> v

        case (k, v: Metrics) =>
          val name = v.displayNameOverride.getOrElse(k)
          name -> v

        case others @ _ => others
      }
    }
  }

  def displayNameOverride: Option[String] = None

  //Only allowed on Master
  def zero(): Unit = {

    this.productIterator.toList.foreach {
      case acc: Acc[_] =>
        acc.reset()
      case _ =>
    }
  }

  case class View[T](
      fn: Metrics.Acc[_ <: AccumulatorV2[_, _]] => Option[T],
      useDisplayName: Boolean = true
  ) {

    def toNestedMap: NestedMap[T] = {
      val result = NestedMap[T]()
      val caseAccessors = getCaseAccessorMap(useDisplayName)
      caseAccessors.foreach {
        case (_name: String, acc: Acc[_]) =>
          val name = acc.name.getOrElse(_name)
          fn(acc).foreach(v => result += name -> Left(v))

        case (_name: String, nested: Metrics) =>
          val name = nested.displayNameOverride.getOrElse(_name)
          val nestedView = nested.View(fn, useDisplayName)
          result += name -> Right(nestedView.toNestedMap)

        case _ =>
          None
      }
      result
    }

    def toMap: Map[String, T] = toNestedMap.leafMap
  }

  object View extends View[Any](v => Some(v.value), true)

  def toNestedMap: NestedMap[Any] = View.toNestedMap
  def toMap: Map[String, Any] = View.toMap
}
