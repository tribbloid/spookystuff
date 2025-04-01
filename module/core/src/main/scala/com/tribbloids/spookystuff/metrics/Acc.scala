package com.tribbloids.spookystuff.metrics

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.EventTimeStatsAccum
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

import scala.language.implicitConversions

/**
  * a simple wrapper of Spark AccumulatorV2 that automatically register itself & derive name from productPrefix
  */
trait Acc[T <: AccumulatorV2[?, ?]] extends MetricLike {

  def sparkContext: SparkContext = SparkSession.active.sparkContext

  def _self: T

  final val self: T = {

    val result = _self
    sparkContext.register(result, displayName)
    result
  }

  final def +=[V0](v: V0)(
      implicit
      canBuild: Acc.CanInit[V0, T]
  ): Unit = {
    canBuild.add(self, v)
  }
}

object Acc {

  implicit def unbox[T <: AccumulatorV2[?, ?]](acc: Acc[T]): T = acc.self

  implicit def boxKV[T <: AccumulatorV2[?, ?]](v: (String, T)): Acc[T] = {
    Simple(
      v._2,
      Some(v._1)
    )
  }

  case class Simple[T <: AccumulatorV2[?, ?]](
      override val _self: T,
      override val displayNameOvrd: Option[String] = None,
      @transient override val sparkContext: SparkContext = SparkSession.active.sparkContext
  ) extends Acc[T]

  case class FromV0[V0, T <: AccumulatorV2[?, ?]](
      v0: V0,
      override val displayNameOvrd: Option[String] = None,
      @transient override val sparkContext: SparkContext = SparkSession.active.sparkContext
  )(
      implicit
      canBuild: CanInit[V0, T]
  ) extends Acc[T] {

    override def _self: T = canBuild.initialise(v0)
  }

  trait CanBuild[T <: AccumulatorV2[?, ?]] extends Serializable {

    def build: T
  }

  object CanBuild extends CanBuild_Level0

  trait CanInit[V0, T <: AccumulatorV2[?, ?]] extends CanBuild[T] {

    def add(self: T, v: V0): Unit

    final def initialise(v: V0): T = {
      val result = build
      add(result, v)
      result
    }
  }

  abstract class CanBuild_Level2 {}

  abstract class CanBuild_Level1 extends CanBuild_Level2 {

    case class Long2Stats[IN]()(
        implicit
        ev: IN => Long
    ) extends CanInit[IN, EventTimeStatsAccum] {

      override def build: EventTimeStatsAccum = new EventTimeStatsAccum()

      override def add(self: EventTimeStatsAccum, v: IN): Unit = self.add(v)
    }
    implicit def long2Stats[IN](
        implicit
        ev: IN => Long
    ): Long2Stats[IN] = Long2Stats()(ev)

    case class FromDouble[IN]()(
        implicit
        ev: IN => Double
    ) extends CanInit[IN, DoubleAccumulator] {
      override def build: DoubleAccumulator = new DoubleAccumulator()

      override def add(self: DoubleAccumulator, v: IN): Unit = self.add(v)
    }
    implicit def double2Double[IN](
        implicit
        ev: IN => Double
    ): FromDouble[IN] = FromDouble()(ev)
  }

  abstract class CanBuild_Level0 extends CanBuild_Level1 {

    case class FromLong[IN]()(
        implicit
        ev: IN => Long
    ) extends CanInit[IN, LongAccumulator] {

      override def build: LongAccumulator = new LongAccumulator()

      override def add(self: LongAccumulator, v: IN): Unit = self.add(v)
    }
    implicit def long2Long[IN](
        implicit
        ev: IN => Long
    ): FromLong[IN] = FromLong()(ev)

    case class FromMap[K, V]() extends CanInit[collection.Map[K, V], MapAccumulator[K, V]] {

      override def build: MapAccumulator[K, V] = MapAccumulator[K, V]()

      override def add(self: MapAccumulator[K, V], map: collection.Map[K, V]): Unit = {

        map.foreach { kv =>
          self.add(kv)
        }
      }

    }
  }

  def create[IN, T <: AccumulatorV2[?, ?]](value: IN, displayNameOvrd: OptionMagnet[String] = None)(
      implicit
      canBuild: CanInit[IN, T]
  ): Acc[T] = {
    FromV0(value, displayNameOvrd)
  }

  implicit def fromV0[IN, T <: AccumulatorV2[?, ?]](value: IN)(
      implicit
      canBuild: CanInit[IN, T]
  ): Acc[T] = {
    create(value)
  }

  implicit def fromKV0[IN, T <: AccumulatorV2[?, ?]](kv: (String, IN))(
      implicit
      canBuild: CanInit[IN, T]
  ): Acc[T] = {

    create(kv._2, kv._1)
  }
}
