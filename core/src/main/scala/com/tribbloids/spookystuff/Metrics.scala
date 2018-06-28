package com.tribbloids.spookystuff

import java.lang

import com.tribbloids.spookystuff.Metrics.Acc
import com.tribbloids.spookystuff.conf.Submodules
import org.apache.spark.SparkContext
import org.apache.spark.ml.dsl.utils.messaging.ProtoAPI
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by peng on 03/10/15.
  */
object Metrics {

  trait CanBuildFrom[T1, IN] extends (T1 => Acc[IN])

  object CanBuildFrom {
    trait BuildLong[T] extends CanBuildFrom[T, lang.Long] {
      override def apply(v1: T) = new Acc(new LongAccumulator()) {
        override def +=(v: Number): Unit = {
          val vv = v.longValue()
          if (vv > 0)
            self.add(v.longValue())
        }
      }
    }
    trait BuildDouble[T] extends CanBuildFrom[T, lang.Double] {
      override def apply(v1: T) = new Acc(new DoubleAccumulator()) {
        override def +=(v: Number): Unit = {
          val vv = v.doubleValue()
          if (vv> 0)
            self.add(v.doubleValue())
        }
      }
    }

    implicit object Long2Long extends BuildLong[Long]
    implicit object JLong2Long extends BuildLong[lang.Long]
    implicit object Int2Long extends BuildLong[Int]

    implicit object Double2Double extends BuildDouble[Double]
    implicit object JDouble2Double extends BuildDouble[lang.Double]
    implicit object Float2Double extends BuildDouble[Float]
  }

  //TODO: is this efficient?
  abstract class Acc[IN](
                          val self: AccumulatorV2[IN, IN]
                        ) extends Serializable {

    def += (v: Number): Unit //adapter that does type cast

    def reset(): Unit = self.reset()
    def name = self.name
    def value = self.value
  }

  def accumulator[T1, IN](
                           value: T1,
                           name: String = null
                         )(
                           implicit
                           canBuildFrom: CanBuildFrom[T1, IN],
                           cast: T1 => Number,
                           sc: SparkContext = SparkContext.getOrCreate()
                         ): Acc[IN] = {

    val acc: Acc[IN] = canBuildFrom(value)
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

  //this is necessary as direct JSON serialization on accumulator only yields meaningless string
  def toTuples: List[(String, Any)] = {
    this.productIterator.toList.flatMap {
      case acc: Acc[_] =>
        acc.name.flatMap {
          v =>
            acc.value match {
              case i: Any => Some(v -> i)
              case _ => None
            }
        }
      case _ =>
        None
    }
  }

  def toMap: ListMap[String, Any] = {
    ListMap(toTuples: _*)
  }

  //Only allowed on Master
  def zero(): Unit = {

    this.productIterator.toList.foreach {
      case acc: Acc[_] =>
        acc.reset()
      case _ =>
    }
  }

  //DO NOT change to val! metrics is very mutable
  override def toMessage_>> : ListMap[String, Any] = {
    val result = toMap
    result
  }

  val children: ArrayBuffer[Metrics] = ArrayBuffer()
}

object SpookyMetrics extends Submodules.Builder[SpookyMetrics] {

  override implicit def default: SpookyMetrics = SpookyMetrics()
}

//TODO: change to multi-level
@SerialVersionUID(64065023841293L)
case class SpookyMetrics(
                          webDriverDispatched: Acc[lang.Long] = Metrics.accumulator(0L, "webDriverDispatched"),
                          webDriverReleased: Acc[lang.Long] = Metrics.accumulator(0L, "webDriverReleased"),

                          pythonDriverDispatched: Acc[lang.Long] = Metrics.accumulator(0L, "pythonDriverDispatched"),
                          pythonDriverReleased: Acc[lang.Long] = Metrics.accumulator(0L, "pythonDriverReleased"),

                          //TODO: cleanup? useless
                          pythonInterpretationSuccess: Acc[lang.Long] = Metrics.accumulator(0L, "pythonInterpretationSuccess"),
                          pythonInterpretationError: Acc[lang.Long] = Metrics.accumulator(0L, "pythonInterpretationSuccess"),

                          sessionInitialized: Acc[lang.Long] = Metrics.accumulator(0L, "sessionInitialized"),
                          sessionReclaimed: Acc[lang.Long] = Metrics.accumulator(0L, "sessionReclaimed"),

                          DFSReadSuccess: Acc[lang.Long] = Metrics.accumulator(0L, "DFSReadSuccess"),
                          DFSReadFailure: Acc[lang.Long] = Metrics.accumulator(0L, "DFSReadFail"),

                          DFSWriteSuccess: Acc[lang.Long] = Metrics.accumulator(0L, "DFSWriteSuccess"),
                          DFSWriteFailure: Acc[lang.Long] = Metrics.accumulator(0L, "DFSWriteFail"),

                          pagesFetched: Acc[lang.Long] = Metrics.accumulator(0L, "pagesFetched"),

                          pagesFetchedFromCache: Acc[lang.Long] = Metrics.accumulator(0L, "pagesFetchedFromCache"),
                          pagesFetchedFromRemote: Acc[lang.Long] = Metrics.accumulator(0L, "pagesFetchedFromRemote"),

                          fetchFromCacheSuccess: Acc[lang.Long] = Metrics.accumulator(0L, "fetchFromCacheSuccess"),
                          fetchFromCacheFailure: Acc[lang.Long] = Metrics.accumulator(0L, "fetchFromCacheFailure"),

                          fetchFromRemoteSuccess: Acc[lang.Long] = Metrics.accumulator(0L, "fetchFromRemoteSuccess"),
                          fetchFromRemoteFailure: Acc[lang.Long] = Metrics.accumulator(0L, "fetchFromRemoteFailure"),

                          pagesSaved: Acc[lang.Long] = Metrics.accumulator(0L, "pagesSaved")

                        ) extends Metrics {
}