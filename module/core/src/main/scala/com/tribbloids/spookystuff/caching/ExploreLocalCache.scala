package com.tribbloids.spookystuff.caching

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.multiverse.CanEqual
import ai.acyclic.prover.commons.util.Caching
import ai.acyclic.prover.commons.util.Caching.{ConcurrentMap, ConcurrentSet}
import com.tribbloids.spookystuff.execution.ExplorePlan.ExeID
import com.tribbloids.spookystuff.execution.{Explore, ExploreRunner}
import com.tribbloids.spookystuff.row.LocalityGroup

/**
  * Singleton, always in the JVM and shared by all executors on the same machine.
  */
object ExploreLocalCache {
  // can it be justified to make this be created on driver and broadcasted to all executors?

  case class Execution[I, O](
      ongoing: ConcurrentSet[ExploreRunner[I, O]] = ConcurrentSet[ExploreRunner[I, O]](), // no eviction
      visited: ConcurrentMap[LocalityGroup, Explore.ReducerTypes[O]#Batch] =
        ConcurrentMap[LocalityGroup, Explore.ReducerTypes[O]#Batch]() // no eviction
  ) extends Explore.Common[I, O] {

    def getVisitedData(key: LocalityGroup): Set[Visited.Batch] = {

      val ongoingVisited: ConcurrentSet[Visited.Batch] = ongoing
        .flatMap { v =>
          v.visited.get(key)
        }

      val committedVisited: Option[Visited.Batch] = visited.get(key)

      ongoingVisited.toSet ++ committedVisited
    }
  }

  val getExecution_untyped = {
    val raw = :=>.at { (_: ExeID) =>
      Execution()
    }
    raw.cached(CanEqual.Native.Lookup(Caching.Soft.build()))
  }

  def getExecution[I, O](v: ExeID): Execution[I, O] = {
    getExecution_untyped(v).asInstanceOf[Execution[I, O]]
  }

  def getOnGoingRunners[I, O](exeID: ExeID): ConcurrentSet[ExploreRunner[I, O]] = {
    //    onGoings.synchronized{
    getExecution(exeID).ongoing
  }

  def register[I, O](v: ExploreRunner[I, O]): Unit = {
    getOnGoingRunners(v.exeID) += v
  }

  def deregister[I, O](v: ExploreRunner[I, O]): Unit = {
    getOnGoingRunners(v.exeID) -= v
  }

  def deregisterAll(exeID: ExeID): Unit = {
    getExecution_untyped.lookup.remove(exeID)
  }
}
