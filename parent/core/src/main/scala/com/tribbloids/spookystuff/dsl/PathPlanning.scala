package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row._

trait PathPlanning {

  type _Impl <: PathPlanning.Impl
  def _Impl: (Params, SpookySchema) => _Impl
}

object PathPlanning {

  trait Impl extends Serializable {

    val params: Params
    val schema: SpookySchema

    def openReducer: DataRow.Reducer

    def openReducer_global: DataRow.Reducer = openReducer

    def selectNextOpen(
        open: ConcurrentMap[LocalityGroup, Vector[DataRow]]
    ): (LocalityGroup, Vector[DataRow])

    def visitedReducer: DataRow.Reducer // precede eliminator

    def visitedReducer_global: DataRow.Reducer = visitedReducer
  }

  object Impl {

    trait CanPruneSelected extends Impl {

      val ordering: RowOrdering

      final def pruneSelected(
          open: Vector[DataRow],
          inCacheVisited: Vector[DataRow]
      ): Vector[DataRow] = {
        if (open.isEmpty || inCacheVisited.isEmpty) open
        else pruneSelectedNonEmpty(open, inCacheVisited)
      }

      protected def pruneSelectedNonEmpty(
          open: Vector[DataRow],
          inCacheVisited: Vector[DataRow]
      ): Vector[DataRow]

      final override def selectNextOpen(
          open: ConcurrentMap[LocalityGroup, Vector[DataRow]]
      ): (LocalityGroup, Vector[DataRow]) = {
        // may return pair with empty DataRows

        // TODO: Should I use pre-sorted collection like SortedMap? Or is it over-engineering?
        val bestOpen: (LocalityGroup, Vector[DataRow]) = open.min(ordering)
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
