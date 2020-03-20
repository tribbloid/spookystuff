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

  def self: T
  def sparkContext: SparkContext = SparkSession.active.sparkContext

  {
    sparkContext.register(self, displayName)
  }

  def +=(v: Number): Unit
}

object Acc {

  implicit def unbox[T <: AccumulatorV2[_, _]](acc: Acc[T]): T = acc.self

  trait CanBuildFrom[V0, T <: AccumulatorV2[_, _]] extends (V0 => T) with Serializable {

    def addNumber(self: T, v: Number): Unit

    case class AccImpl(
        self: T,
        override val displayNameOvrd: Option[String] = None
    ) extends Acc[T] {

      override def +=(v: Number): Unit = addNumber(self, v)
    }
  }

  abstract class CanBuildFrom_Level2 {}

  abstract class CanBuildFrom_Level1 extends CanBuildFrom_Level2 {

    case class Long2Stats[IN](implicit ev: IN => Long) extends CanBuildFrom[IN, EventTimeStatsAccum] {
      override def apply(v1: IN): EventTimeStatsAccum = {
        val result = new EventTimeStatsAccum()
        result.add(v1)
        result
      }

      override def addNumber(self: EventTimeStatsAccum, v: Number): Unit = self.add(v.longValue)
    }
    implicit def long2Stats[IN](implicit ev: IN => Long): Long2Stats[IN] = Long2Stats()(ev)

    case class Double2Double[IN](implicit ev: IN => Double) extends CanBuildFrom[IN, DoubleAccumulator] {
      override def apply(v1: IN): DoubleAccumulator = {
        val result = new DoubleAccumulator()
        result.add(v1)
        result
      }

      override def addNumber(self: DoubleAccumulator, v: Number): Unit = self.add(v.doubleValue)
    }
    implicit def double2Double[IN](implicit ev: IN => Double): Double2Double[IN] = Double2Double()(ev)
  }

  object CanBuildFrom extends CanBuildFrom_Level1 {

    case class Long2Long[IN](implicit ev: IN => Long) extends CanBuildFrom[IN, LongAccumulator] {
      override def apply(v1: IN): LongAccumulator = {
        val result = new LongAccumulator()
        result.add(v1)
        result
      }

      override def addNumber(self: LongAccumulator, v: Number): Unit = self.add(v.longValue)
    }
    implicit def long2Long[IN](implicit ev: IN => Long): Long2Long[IN] = Long2Long()(ev)

  }

  def create[IN, T <: AccumulatorV2[_, _]](value: IN, displayNameOvrd: String ? _ = None)(
      implicit canBuildFrom: CanBuildFrom[IN, T]
  ): Acc[T] = {
    canBuildFrom.AccImpl(value, displayNameOvrd.asOption)
  }

  implicit def fromV0[IN, T <: AccumulatorV2[_, _]](value: IN)(
      implicit canBuildFrom: CanBuildFrom[IN, T]
  ): Acc[T] = {
    create(value)
  }

  implicit def fromKV0[IN, T <: AccumulatorV2[_, _]](kv: (String, IN))(
      implicit canBuildFrom: CanBuildFrom[IN, T]
  ): Acc[T] = {

    create(kv._2, kv._1)
  }
}
