package com.tribbloids.spookystuff.caching

import ai.acyclic.prover.commons.util.Caching.Soft.View
import ai.acyclic.prover.commons.util.Caching.{ConcurrentMap, ConcurrentSet}
import com.tribbloids.spookystuff.execution.ExplorePlan.ExeID
import com.tribbloids.spookystuff.execution.ExploreRunner
import com.tribbloids.spookystuff.row.{DataRow, LocalityGroup}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine.
  */
object ExploreLocalCache {

  // (Trace, ExecutionID) -> Squashed Rows
  // exeID is used to segment Squashed Rows from different jobs
  val committedVisited: View[(LocalityGroup, ExeID), Vector[DataRow]] =
    View()

  val onGoings: ConcurrentMap[ExeID, ConcurrentSet[ExploreRunner]] =
    ConcurrentMap() // executionID -> running ExploreStateView

  def getOnGoingRunners(exeID: ExeID): ConcurrentSet[ExploreRunner] = {
    //    onGoings.synchronized{
    onGoings
      .getOrElseUpdate(
        exeID, {
          val v = ConcurrentSet[ExploreRunner]()
          v
        }
      )
  }

  def commitVisited(
                     v: ExploreRunner,
                     reducer: DataRow.Reducer
  ): Unit = {

    // TODO relax synchronized check to accelerate?
    def commit1(
        key: (LocalityGroup, ExeID),
        value: Vector[DataRow],
        reducer: DataRow.Reducer
    ): Unit = {

      committedVisited.synchronized {
        val oldVs = committedVisited.get(key)
        val newVs = (Seq(value) ++ oldVs).reduce(reducer)
        committedVisited.put(key, newVs)
      }
    }

    val toBeCommitted = v.visited
      .map { tuple =>
        (tuple._1 -> v.pathPlanningImpl.params.executionID) -> tuple._2
      }

    toBeCommitted.foreach { kv =>
      commit1(kv._1, kv._2, reducer)
    }
  }

  def register(v: ExploreRunner): Unit = {
    getOnGoingRunners(v.exeID) += v
  }

  def deregister(v: ExploreRunner): Unit = {
    getOnGoingRunners(v.exeID) -= v
  }

  def deregisterAll(exeID: ExeID): Unit = {
    onGoings -= exeID
  }

  def get(key: (LocalityGroup, ExeID)): Set[Vector[DataRow]] = {
    val onGoing = this
      .getOnGoingRunners(key._2)

    val onGoingVisitedSet = onGoing
      .flatMap { v =>
        v.visited.get(key._1)
      }

    onGoingVisitedSet.toSet ++ committedVisited.get(key)
  }

  @deprecated // TODO: remove, useless
  def getAll(exeID: ExeID): Map[LocalityGroup, Vector[DataRow]] = {
    val onGoing: Map[LocalityGroup, Vector[DataRow]] = this
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
