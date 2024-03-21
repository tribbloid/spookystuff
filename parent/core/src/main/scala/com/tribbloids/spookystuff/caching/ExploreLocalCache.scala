package com.tribbloids.spookystuff.caching

import ai.acyclic.prover.commons.function
import ai.acyclic.prover.commons.function.Impl
import ai.acyclic.prover.commons.same.Same
import ai.acyclic.prover.commons.util.Caching
import ai.acyclic.prover.commons.util.Caching.{ConcurrentMap, ConcurrentSet}
import com.tribbloids.spookystuff.execution.ExplorePlan.ExeID
import com.tribbloids.spookystuff.execution.{ExploreRunner, ExploreSupport}
import com.tribbloids.spookystuff.row.{Data, LocalityGroup}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine.
  */
object ExploreLocalCache {
  // can it be justified to make this be created on driver and broadcasted to all executors?

  case class Execution(
      ongoing: ConcurrentSet[ExploreRunner] = ConcurrentSet(), // no eviction
      visited: ConcurrentMap[LocalityGroup, Vector[DataRow]] = ConcurrentMap() // no eviction
  ) {

    def getData(key: LocalityGroup): Set[Vector[Data.WithLineage[_]]] = {

      val ongoingVisited = ongoing
        .flatMap { v =>
          v.visited.get(key)
        }

      val committedVisited = visited.get(key)

      ongoingVisited.toSet ++ committedVisited
    }
  }

  val getExecution: function.HomSystem.FnImpl.Cached[ExeID, Execution] = {
    val raw = Impl { _: ExeID =>
      Execution()
    }
    raw.cachedBy(Same.ByEquality.Lookup(Caching.Soft.build()))
  }

  def getOnGoingRunners(exeID: ExeID): ConcurrentSet[ExploreRunner[D]] = {
    //    onGoings.synchronized{
    getExecution(exeID).ongoing
  }

  def commitVisited(
      v: ExploreRunner[D],
      reducer: Reducer
  ): Unit = {

    // TODO relax synchronized check to accelerate?
    def commit1(
        key: (LocalityGroup, ExeID),
        value: Vector[Lineage],
        reducer: Reducer
    ): Unit = {

      val exe = getExecution(key._2)

      exe.visited.synchronized {
        val oldVs = exe.visited.get(key._1)
        val newVs = (Seq(value) ++ oldVs).reduce(reducer)
        exe.visited.put(key._1, newVs)
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

  def register(v: ExploreRunner[D]): Unit = {
    getOnGoingRunners(v.exeID) += v
  }

  def deregister(v: ExploreRunner[D]): Unit = {
    getOnGoingRunners(v.exeID) -= v
  }

  def deregisterAll(exeID: ExeID): Unit = {
    getExecution.underlyingCache.remove(exeID)
  }
}
