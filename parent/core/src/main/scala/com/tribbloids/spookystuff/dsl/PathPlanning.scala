package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.Explore
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row.*

trait PathPlanning {

  type _Impl[I, O] <: PathPlanning.Impl[I, O]
  def _Impl[I, O](params: Params, schema: SpookySchema): _Impl[I, O]
}

object PathPlanning {

  trait Impl[I, O] extends Explore.Common[I, O] with Serializable {

    val params: Params
    val schema: SpookySchema

    def openReducer: OpenReducer

    def openReducer_global: OpenReducer = openReducer

    def selectNextOpen(
        open: ConcurrentMap[LocalityGroup, Vector[Exploring]]
    ): (LocalityGroup, Vector[Exploring])

    def visitedReducer: VisitedReducer // precede eliminator

    def visitedReducer_global: VisitedReducer = visitedReducer
  }

  object Impl {

    trait CanPruneSelected[I, O] extends Impl[I, O] {

      val ordering: RowOrdering // TODO: over-defined, only need to implement min selection

      final def pruneSelected(
          open: Batch,
          inCacheVisited: _Batch
      ): Vector[Exploring] = {
        if (open.isEmpty || inCacheVisited.isEmpty) open
        else pruneSelectedNonEmpty(open, inCacheVisited)
      }

      protected def pruneSelectedNonEmpty(
          open: Batch,
          inCacheVisited: _Batch
      ): Vector[Exploring]

      final override def selectNextOpen(
          open: ConcurrentMap[LocalityGroup, Vector[Exploring]]
      ): (LocalityGroup, Vector[Exploring]) = {
        // may return pair with empty DataRows

        // TODO: Should I use pre-sorted collection like SortedMap? Or is it over-engineering?
        val bestOpen: (LocalityGroup, Vector[Exploring]) = open.min(ordering)
        val bestOpenGroup = bestOpen._1

        open -= bestOpenGroup

        val allVisitedOpt = {

          val cached: Set[_Batch] = ExploreLocalCache
            .getExecution[I, O](params.executionID)
            .getVisitedData(bestOpenGroup)

          cached
            .reduceOption(visitedReducer)
        }

        val selected = allVisitedOpt match {
          case Some(allVisited) =>
            val dataRowsAfterPruning = pruneSelected(bestOpen._2, allVisited)

            bestOpen.copy(_2 = dataRowsAfterPruning)
          case None =>
            bestOpen
        }

        selected
      }
    }
  }

}
