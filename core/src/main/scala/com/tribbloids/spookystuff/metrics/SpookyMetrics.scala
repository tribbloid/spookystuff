package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.utils.accumulator.MapAccumulator
import org.apache.spark.util.LongAccumulator

import scala.language.implicitConversions

object SpookyMetrics {}

@SerialVersionUID(64065023841293L)
case class SpookyMetrics(
    sessionInitialized: Acc[LongAccumulator] = Acc.create(0L, "sessionInitialized"),
    sessionReclaimed: Acc[LongAccumulator] = Acc.create(0L, "sessionReclaimed"),
    //
    driverDispatched: Acc[MapAccumulator[String, Long]] = "driverDispatched" -> MapAccumulator.kToLong[String],
    driverReleased: Acc[MapAccumulator[String, Long]] = "driverReleased" -> MapAccumulator.kToLong[String],
    //
    DFSReadSuccess: Acc[LongAccumulator] = Acc.create(0L, "DFSReadSuccess"),
    DFSReadFailure: Acc[LongAccumulator] = Acc.create(0L, "DFSReadFail"),
    DFSWriteSuccess: Acc[LongAccumulator] = Acc.create(0L, "DFSWriteSuccess"),
    DFSWriteFailure: Acc[LongAccumulator] = Acc.create(0L, "DFSWriteFail"),
    pagesFetched: Acc[LongAccumulator] = Acc.create(0L, "pagesFetched"),
    pagesFetchedFromCache: Acc[LongAccumulator] = Acc.create(0L, "pagesFetchedFromCache"),
    pagesFetchedFromRemote: Acc[LongAccumulator] = Acc.create(0L, "pagesFetchedFromRemote"),
    fetchFromCacheSuccess: Acc[LongAccumulator] = Acc.create(0L, "fetchFromCacheSuccess"),
    fetchFromCacheFailure: Acc[LongAccumulator] = Acc.create(0L, "fetchFromCacheFailure"),
    fetchFromRemoteSuccess: Acc[LongAccumulator] = Acc.create(0L, "fetchFromRemoteSuccess"),
    fetchFromRemoteFailure: Acc[LongAccumulator] = Acc.create(0L, "fetchFromRemoteFailure"),
    pagesSaved: Acc[LongAccumulator] = Acc.create(0L, "pagesSaved")
) extends AbstractMetrics {

  object Drivers extends Serializable {

    def dispatchedTotalCount: Long = driverDispatched.value.values.sum
    def releasedTotalCount: Long = driverReleased.value.values.sum
  }
}
