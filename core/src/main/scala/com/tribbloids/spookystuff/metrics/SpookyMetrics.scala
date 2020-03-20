package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.conf.Submodules
import org.apache.spark.util.LongAccumulator

import scala.language.implicitConversions

object SpookyMetrics extends Submodules.Builder[SpookyMetrics] {

  override implicit def default: SpookyMetrics = SpookyMetrics()
}

//TODO: change to multi-level
@SerialVersionUID(64065023841293L)
case class SpookyMetrics(
    webDriverDispatched: Acc[LongAccumulator] = Acc.create(0L, "webDriverDispatched"),
    webDriverReleased: Acc[LongAccumulator] = Acc.create(0L, "webDriverReleased"),
    pythonDriverDispatched: Acc[LongAccumulator] = Acc.create(0L, "pythonDriverDispatched"),
    pythonDriverReleased: Acc[LongAccumulator] = Acc.create(0L, "pythonDriverReleased"),
    //TODO: cleanup? useless
    pythonInterpretationSuccess: Acc[LongAccumulator] = Acc.create(0L, "pythonInterpretationSuccess"),
    pythonInterpretationError: Acc[LongAccumulator] = Acc.create(0L, "pythonInterpretationSuccess"),
    sessionInitialized: Acc[LongAccumulator] = Acc.create(0L, "sessionInitialized"),
    sessionReclaimed: Acc[LongAccumulator] = Acc.create(0L, "sessionReclaimed"),
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
) extends Metrics {}
