package com.tribbloids.spookystuff

import org.apache.spark.ml.dsl.utils.Message
import org.apache.spark.{Accumulator, AccumulatorParam, SparkContext}

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by peng on 03/10/15.
  */
object SpookyMetrics {

  def accumulator[T](
                      initialValue: T,
                      name: String,
                      scOpt: Option[SparkContext] = None
                    )(implicit param: AccumulatorParam[T]): Accumulator[T] = {
    val sc = scOpt.getOrElse(SparkContext.getOrCreate())
    sc.accumulator(initialValue, name)
  }
}

@SerialVersionUID(-32509237409L)
abstract class AbstractMetrics extends Message with Product with Serializable {

  //this is necessary as direct JSON serialization on accumulator only yields meaningless string
  def toTuples: List[(String, Any)] = {
    this.productIterator.toList.flatMap {
      case acc: Accumulator[_] =>
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
      case acc: Accumulator[_] =>
        if (acc.value.isInstanceOf[Int])
          acc.asInstanceOf[Accumulator[Int]].setValue(0)
      case _ =>
    }
  }

  override val value: ListMap[String, Any] = toMap

  val children: ArrayBuffer[AbstractMetrics] = ArrayBuffer()
}

//TODO: change to multi-level
@SerialVersionUID(64065023841293L)
case class SpookyMetrics(
                          webDriverDispatched: Accumulator[Int] = SpookyMetrics.accumulator(0, "webDriverDispatched"),
                          webDriverReleased: Accumulator[Int] = SpookyMetrics.accumulator(0, "webDriverReleased"),

                          pythonDriverDispatched: Accumulator[Int] = SpookyMetrics.accumulator(0, "pythonDriverDispatched"),
                          pythonDriverReleased: Accumulator[Int] = SpookyMetrics.accumulator(0, "pythonDriverReleased"),

                          //TODO: cleanup? useless
                          pythonInterpretationSuccess: Accumulator[Int] = SpookyMetrics.accumulator(0, "pythonInterpretationSuccess"),
                          pythonInterpretationError: Accumulator[Int] = SpookyMetrics.accumulator(0, "pythonInterpretationSuccess"),

                          sessionInitialized: Accumulator[Int] = SpookyMetrics.accumulator(0, "sessionInitialized"),
                          sessionReclaimed: Accumulator[Int] = SpookyMetrics.accumulator(0, "sessionReclaimed"),

                          DFSReadSuccess: Accumulator[Int] = SpookyMetrics.accumulator(0, "DFSReadSuccess"),
                          DFSReadFailure: Accumulator[Int] = SpookyMetrics.accumulator(0, "DFSReadFail"),

                          DFSWriteSuccess: Accumulator[Int] = SpookyMetrics.accumulator(0, "DFSWriteSuccess"),
                          DFSWriteFailure: Accumulator[Int] = SpookyMetrics.accumulator(0, "DFSWriteFail"),

                          pagesFetched: Accumulator[Int] = SpookyMetrics.accumulator(0, "pagesFetched"),

                          pagesFetchedFromCache: Accumulator[Int] = SpookyMetrics.accumulator(0, "pagesFetchedFromCache"),
                          pagesFetchedFromRemote: Accumulator[Int] = SpookyMetrics.accumulator(0, "pagesFetchedFromRemote"),

                          fetchFromCacheSuccess: Accumulator[Int] = SpookyMetrics.accumulator(0, "fetchFromCacheSuccess"),
                          fetchFromCacheFailure: Accumulator[Int] = SpookyMetrics.accumulator(0, "fetchFromCacheFailure"),

                          fetchFromRemoteSuccess: Accumulator[Int] = SpookyMetrics.accumulator(0, "fetchFromRemoteSuccess"),
                          fetchFromRemoteFailure: Accumulator[Int] = SpookyMetrics.accumulator(0, "fetchFromRemoteFailure"),

                          pagesSaved: Accumulator[Int] = SpookyMetrics.accumulator(0, "pagesSaved"),

                          //TODO: move to MAV component
//                          proxyCreated: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),
//                          proxyDestroyed: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),

                          linkCreated: Accumulator[Int] = SpookyMetrics.accumulator(0, "linkCreated"),
                          linkDestroyed: Accumulator[Int] = SpookyMetrics.accumulator(0, "linkDestroyed")
                        ) extends AbstractMetrics {
}