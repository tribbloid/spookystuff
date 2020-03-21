package com.tribbloids.spookystuff.metrics

import org.apache.spark.SparkContext
import org.apache.spark.ml.dsl.utils.?
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.EventTimeStatsAccum
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

import scala.language.{existentials, implicitConversions}

/**
  * a simple wrapper of Spark AccumulatorV2 that automatically register itself & derive name from productPrefix
  */
trait Acc[T <: AccumulatorV2[_, _]] extends MetricLike {

  def sparkContext: SparkContext = SparkSession.active.sparkContext

  def _self: T

  final val self = {

    val result = _self
    sparkContext.register(result, displayName)
    result
  }

  final def +=[V0](v: V0)(implicit canBuild: Acc.CanBuild[V0, T]): Unit = {
    canBuild.add(self, v)
  }
}

object Acc {

  implicit def unbox[T <: AccumulatorV2[_, _]](acc: Acc[T]): T = acc.self

  case class FromNether[T <: AccumulatorV2[_, _]](
      override val displayNameOvrd: Option[String] = None,
      @transient override val sparkContext: SparkContext = SparkSession.active.sparkContext
  )(
      implicit canBuildFrom: CanBuild[_, T]
  ) extends Acc[T] {

    override def _self: T = canBuildFrom.newInstance
  }

  case class FromV0[V0, T <: AccumulatorV2[_, _]](
      v0: V0,
      override val displayNameOvrd: Option[String] = None,
      @transient override val sparkContext: SparkContext = SparkSession.active.sparkContext
  )(
      implicit canBuildFrom: CanBuild[V0, T]
  ) extends Acc[T] {

    override def _self: T = canBuildFrom.build(v0)
  }

  trait CanBuild[V0, T <: AccumulatorV2[_, _]] extends Serializable {

    def newInstance: T

    def add(self: T, v: V0): Unit

    final def build(v: V0): T = {
      val result = newInstance
      add(result, v)
      result
    }
  }

  abstract class CanBuild_Level2 {}

  abstract class CanBuild_Level1 extends CanBuild_Level2 {

    case class Long2Stats[IN](implicit ev: IN => Long) extends CanBuild[IN, EventTimeStatsAccum] {

      override def newInstance: EventTimeStatsAccum = new EventTimeStatsAccum()

      override def add(self: EventTimeStatsAccum, v: IN): Unit = self.add(v)
    }
    implicit def long2Stats[IN](implicit ev: IN => Long): Long2Stats[IN] = Long2Stats()(ev)

    case class Double2Double[IN](implicit ev: IN => Double) extends CanBuild[IN, DoubleAccumulator] {
      override def newInstance: DoubleAccumulator = new DoubleAccumulator()

      override def add(self: DoubleAccumulator, v: IN): Unit = self.add(v)
    }
    implicit def double2Double[IN](implicit ev: IN => Double): Double2Double[IN] = Double2Double()(ev)
  }

  object CanBuild extends CanBuild_Level1 {

    case class Long2Long[IN](implicit ev: IN => Long) extends CanBuild[IN, LongAccumulator] {

      override def newInstance: LongAccumulator = new LongAccumulator()

      override def add(self: LongAccumulator, v: IN): Unit = self.add(v)
    }
    implicit def long2Long[IN](implicit ev: IN => Long): Long2Long[IN] = Long2Long()(ev)

  }

  def create[IN, T <: AccumulatorV2[_, _]](value: IN, displayNameOvrd: String ? _ = None)(
      implicit canBuildFrom: CanBuild[IN, T]
  ): Acc[T] = {
    FromV0(value, displayNameOvrd.asOption)
  }

  implicit def fromV0[IN, T <: AccumulatorV2[_, _]](value: IN)(
      implicit canBuildFrom: CanBuild[IN, T]
  ): Acc[T] = {
    create(value)
  }

  implicit def fromKV0[IN, T <: AccumulatorV2[_, _]](kv: (String, IN))(
      implicit canBuildFrom: CanBuild[IN, T]
  ): Acc[T] = {

    create(kv._2, kv._1)
  }
}
