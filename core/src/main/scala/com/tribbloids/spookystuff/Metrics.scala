package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.{Accumulator, AccumulatorParam}

import scala.collection.immutable.ListMap

/**
  * Created by peng on 03/10/15.
  */
object Metrics {

  private def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }
}

//TODO: change to multi-level
case class Metrics(
                    webDriverDispatched: Accumulator[Int] = Metrics.accumulator(0, "webDriverDispatched"),
                    webDriverReleased: Accumulator[Int] = Metrics.accumulator(0, "webDriverReleased"),

                    pythonDriverDispatched: Accumulator[Int] = Metrics.accumulator(0, "pythonDriverDispatched"),
                    pythonDriverReleased: Accumulator[Int] = Metrics.accumulator(0, "pythonDriverReleased"),

                    sessionInitialized: Accumulator[Int] = Metrics.accumulator(0, "sessionInitialized"),
                    sessionReclaimed: Accumulator[Int] = Metrics.accumulator(0, "sessionReclaimed"),

                    DFSReadSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSReadSuccess"),
                    DFSReadFailure: Accumulator[Int] = Metrics.accumulator(0, "DFSReadFail"),

                    DFSWriteSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteSuccess"),
                    DFSWriteFailure: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteFail"),

                    pagesFetched: Accumulator[Int] = Metrics.accumulator(0, "pagesFetched"),

                    pagesFetchedFromCache: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromCache"),
                    pagesFetchedFromRemote: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromRemote"),

                    fetchFromCacheSuccess: Accumulator[Int] = Metrics.accumulator(0, "fetchFromCacheSuccess"),
                    fetchFromCacheFailure: Accumulator[Int] = Metrics.accumulator(0, "fetchFromCacheFailure"),

                    fetchFromRemoteSuccess: Accumulator[Int] = Metrics.accumulator(0, "fetchFromRemoteSuccess"),
                    fetchFromRemoteFailure: Accumulator[Int] = Metrics.accumulator(0, "fetchFromRemoteFailure"),

                    pagesSaved: Accumulator[Int] = Metrics.accumulator(0, "pagesSaved")
                  ) {

  def toJSON: String = {

    val map = ListMap(toTuples: _*)

    SpookyUtils.toJson(map, beautiful = true)
  }

  //this is necessary as direct JSON serialization on accumulator only yields meaningless string
  def toTuples: Seq[(String, Any)] = {
    this.productIterator.flatMap {
      case acc: Accumulator[_] => acc.name.map(_ -> acc.value)
      case _ => None
    }.toSeq
  }
}