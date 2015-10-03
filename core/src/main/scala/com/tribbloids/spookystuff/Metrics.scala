package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.utils.Utils
import org.apache.spark.{AccumulatorParam, Accumulator}

/**
 * Created by peng on 03/10/15.
 */
object Metrics {

  private def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }
}

case class Metrics(
                    driverInitialized: Accumulator[Int] = Metrics.accumulator(0, "driverInitialized"),
                    driverReclaimed: Accumulator[Int] = Metrics.accumulator(0, "driverReclaimed"),

                    sessionInitialized: Accumulator[Int] = Metrics.accumulator(0, "sessionInitialized"),
                    sessionReclaimed: Accumulator[Int] = Metrics.accumulator(0, "sessionReclaimed"),

                    DFSReadSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSReadSuccess"),
                    DFSReadFailure: Accumulator[Int] = Metrics.accumulator(0, "DFSReadFail"),

                    DFSWriteSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteSuccess"),
                    DFSWriteFailure: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteFail"),

                    pagesFetched: Accumulator[Int] = Metrics.accumulator(0, "pagesFetched"),
                    pagesFetchedFromWeb: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromWeb"),
                    pagesFetchedFromCache: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromCache"),

                    fetchSuccess: Accumulator[Int] = Metrics.accumulator(0, "fetchSuccess"),
                    fetchFailure: Accumulator[Int] = Metrics.accumulator(0, "fetchFailure"),

                    pagesSaved: Accumulator[Int] = Metrics.accumulator(0, "pagesSaved")
                    ) {

  def toJSON: String = {
    //    val tuples = this.productIterator.flatMap{
    //      case acc: Accumulator[_] => acc.name.map(_ -> acc.value)
    //      case _ => None
    //    }.toSeq
    //
    //    val map = ListMap(tuples: _*)

    Utils.toJson(this, beautiful = true)
  }
}