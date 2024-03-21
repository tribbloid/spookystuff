package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.ExploreSupport
import com.tribbloids.spookystuff.row._

trait PathPlanning {

  type _Impl[D] <: PathPlanning.Impl[D]
  def _Impl[D](params: Params, schema: SpookySchema[D]): _Impl[D]
}

object PathPlanning {

  trait Impl[D] extends ExploreSupport[D] with Serializable {

    val params: Params
    val schema: SpookySchema[D]

    def openReducer: Reducer

    def openReducer_global: Reducer = openReducer

    def selectNextOpen(
        open: ConcurrentMap[LocalityGroup, Vector[Lineage]]
    ): (LocalityGroup, Vector[Lineage])

    def visitedReducer: Reducer // precede eliminator

    def visitedReducer_global: Reducer = visitedReducer
  }

  object Impl {

    trait CanPruneSelected[D] extends Impl[D] {

      val ordering: RowOrdering

      final def pruneSelected(
          open: Vector[Lineage],
          inCacheVisited: Vector[Lineage]
      ): Vector[Lineage] = {
        if (open.isEmpty || inCacheVisited.isEmpty) open
        else pruneSelectedNonEmpty(open, inCacheVisited)
      }

      protected def pruneSelectedNonEmpty(
          open: Vector[Lineage],
          inCacheVisited: Vector[Lineage]
      ): Vector[Lineage]

      final override def selectNextOpen(
          open: ConcurrentMap[LocalityGroup, Vector[Lineage]]
      ): (LocalityGroup, Vector[Lineage]) = {
        // may return pair with empty DataRows

        // TODO: Should I use pre-sorted collection like SortedMap? Or is it over-engineering?
        val bestOpen: (LocalityGroup, Vector[Lineage]) = open.min(ordering)
        val bestOpenGroup = bestOpen._1

        open -= bestOpenGroup

        val allVisitedOpt =
          ExploreLocalCache
            .getExecution(params.executionID)
            .getData(bestOpenGroup)
            .reduceOption(visitedReducer)

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
