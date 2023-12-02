package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.execution.ExplorePlan.ExeID
import com.tribbloids.spookystuff.execution.ExploreAlgorithmRunner
import com.tribbloids.spookystuff.row.{DataRow, RowReducer}
import com.tribbloids.spookystuff.utils.Caching
import com.tribbloids.spookystuff.utils.Caching.{ConcurrentMap, ConcurrentSet}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine This is a makeshift implementation,
  * faster implementation will be based on Google Guava library
  */
object ExploreRunnerCache {

  // (TraceView, ExecutionID) -> Squashed Rows
  // exeID is used to segment Squashed Rows from different jobs
  val committedVisited: Caching.ConcurrentCache[(Trace, ExeID), Vector[DataRow]] =
    Caching.ConcurrentCache()

  val onGoings: ConcurrentMap[ExeID, ConcurrentSet[ExploreAlgorithmRunner]] =
    Caching.ConcurrentMap() // executionID -> running ExploreStateView

  def getOnGoingRunners(exeID: ExeID): ConcurrentSet[ExploreAlgorithmRunner] = {
    //    onGoings.synchronized{
    onGoings
      .getOrElseUpdate(
        exeID, {
          val v = ConcurrentSet[ExploreAlgorithmRunner]()
          v
        }
      )
  }

  def finishExploreExecutions(exeID: ExeID): Unit = {
    onGoings -= exeID
  }

  // TODO relax synchronized check to accelerate?
  private def commit1(
      key: (Trace, ExeID),
      value: Vector[DataRow],
      reducer: RowReducer
  ): Unit = {

    committedVisited.synchronized {
      val oldVs = committedVisited.get(key)
      val newVs = (Seq(value) ++ oldVs).reduce(reducer)
      committedVisited.put(key, newVs)
    }
  }

  def commit(
      kvs: Iterable[((Trace, ExeID), Vector[DataRow])],
      reducer: RowReducer
  ): Unit = {

    kvs.foreach { kv =>
      commit1(kv._1, kv._2, reducer)
    }
  }

  def register(v: ExploreAlgorithmRunner, exeID: ExeID): Unit = {
    getOnGoingRunners(exeID) += v
  }

  def deregister(v: ExploreAlgorithmRunner, exeID: ExeID): Unit = {
    getOnGoingRunners(exeID) -= v
  }

  def get(key: (Trace, ExeID)): Set[Vector[DataRow]] = {
    val onGoing = this
      .getOnGoingRunners(key._2)

    val onGoingVisitedSet = onGoing
      .flatMap { v =>
        v.visited.get(key._1)
      }

    onGoingVisitedSet.toSet ++ committedVisited.get(key)
  }

  def getAll(exeID: ExeID): Map[Trace, Vector[DataRow]] = {
    val onGoing: Map[Trace, Vector[DataRow]] = this
      .getOnGoingRunners(exeID)
      .map(_.visited.toMap)
      .reduceOption { (v1, v2) =>
        v1 ++ v2
      }
      .getOrElse(Map.empty)

    val commited = committedVisited.toMap.view
      .filterKeys(_._2 == exeID)
      .map {
        case (k, v) =>
          k._1 -> v
      }

    onGoing ++ commited
  }
}
