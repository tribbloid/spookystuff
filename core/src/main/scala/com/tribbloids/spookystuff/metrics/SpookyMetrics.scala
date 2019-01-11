package com.tribbloids.spookystuff.metrics
import com.tribbloids.spookystuff.conf.Submodules
import com.tribbloids.spookystuff.metrics.Metrics.Acc
import org.apache.spark.util.LongAccumulator

import scala.language.implicitConversions

object SpookyMetrics extends Submodules.Builder[SpookyMetrics] {

  override implicit def default: SpookyMetrics = SpookyMetrics()
}

//TODO: change to multi-level
@SerialVersionUID(64065023841293L)
case class SpookyMetrics(
    webDriverDispatched: Acc[LongAccumulator] = Metrics.accumulator(0L, "webDriverDispatched"),
    webDriverReleased: Acc[LongAccumulator] = Metrics.accumulator(0L, "webDriverReleased"),
    pythonDriverDispatched: Acc[LongAccumulator] = Metrics.accumulator(0L, "pythonDriverDispatched"),
    pythonDriverReleased: Acc[LongAccumulator] = Metrics.accumulator(0L, "pythonDriverReleased"),
    //TODO: cleanup? useless
    pythonInterpretationSuccess: Acc[LongAccumulator] = Metrics.accumulator(0L, "pythonInterpretationSuccess"),
    pythonInterpretationError: Acc[LongAccumulator] = Metrics.accumulator(0L, "pythonInterpretationSuccess"),
    sessionInitialized: Acc[LongAccumulator] = Metrics.accumulator(0L, "sessionInitialized"),
    sessionReclaimed: Acc[LongAccumulator] = Metrics.accumulator(0L, "sessionReclaimed"),
    DFSReadSuccess: Acc[LongAccumulator] = Metrics.accumulator(0L, "DFSReadSuccess"),
    DFSReadFailure: Acc[LongAccumulator] = Metrics.accumulator(0L, "DFSReadFail"),
    DFSWriteSuccess: Acc[LongAccumulator] = Metrics.accumulator(0L, "DFSWriteSuccess"),
    DFSWriteFailure: Acc[LongAccumulator] = Metrics.accumulator(0L, "DFSWriteFail"),
    pagesFetched: Acc[LongAccumulator] = Metrics.accumulator(0L, "pagesFetched"),
    pagesFetchedFromCache: Acc[LongAccumulator] = Metrics.accumulator(0L, "pagesFetchedFromCache"),
    pagesFetchedFromRemote: Acc[LongAccumulator] = Metrics.accumulator(0L, "pagesFetchedFromRemote"),
    fetchFromCacheSuccess: Acc[LongAccumulator] = Metrics.accumulator(0L, "fetchFromCacheSuccess"),
    fetchFromCacheFailure: Acc[LongAccumulator] = Metrics.accumulator(0L, "fetchFromCacheFailure"),
    fetchFromRemoteSuccess: Acc[LongAccumulator] = Metrics.accumulator(0L, "fetchFromRemoteSuccess"),
    fetchFromRemoteFailure: Acc[LongAccumulator] = Metrics.accumulator(0L, "fetchFromRemoteFailure"),
    pagesSaved: Acc[LongAccumulator] = Metrics.accumulator(0L, "pagesSaved")
) extends Metrics {}
