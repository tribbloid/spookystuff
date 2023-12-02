package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.execution.ExplorePlan.{Open_Visited, Params}
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.Caching.ConcurrentMap

trait ExploreAlgorithm {

  def getImpl(
      params: Params,
      schema: SpookySchema
  ): ExploreAlgorithm.Impl
}

object ExploreAlgorithm {

  trait Impl extends Serializable {

    val params: Params
    val schema: SpookySchema

    def openReducer: RowReducer

    def openReducer_global: RowReducer = openReducer

    def selectNext(
        open: ConcurrentMap[Trace, Vector[DataRow]]
    ): (Trace, Vector[DataRow])

    def visitedReducer: RowReducer // precede eliminator

    def visitedReducer_global: RowReducer = visitedReducer

    def combine(open: RowReducer, visited: RowReducer): (Open_Visited, Open_Visited) => Open_Visited = { (v1, v2) =>
      Open_Visited(
        open = (v1.open ++ v2.open).reduceOption(open).map(_.toVector),
        visited = (v1.visited ++ v2.visited)
          .reduceOption(visited)
          .map(_.toVector)
      )
    }

    @transient lazy val combineLocally: (Open_Visited, Open_Visited) => Open_Visited =
      combine(openReducer, visitedReducer)

    @transient lazy val combineGlobally: (Open_Visited, Open_Visited) => Open_Visited =
      combine(openReducer_global, visitedReducer_global)

  }

  trait EliminatingImpl extends Impl {

    val ordering: RowOrdering

    final def pruneOpen(
        open: Vector[DataRow],
        visited: Vector[DataRow]
    ): Vector[DataRow] = {
      if (open.isEmpty || visited.isEmpty) open
      else pruneOpenNonEmpty(open, visited)
    }

    protected def pruneOpenNonEmpty(
        open: Vector[DataRow],
        visited: Vector[DataRow]
    ): Vector[DataRow]

    final override def selectNext(
        open: ConcurrentMap[Trace, Vector[DataRow]]
    ): (Trace, Vector[DataRow]) = {

      // TODO: Should I use pre-sorted collection? Or is it over-engineering?
      val bestOpenBeforeElimination = open.min(ordering)
      val bestOpenNodeID = bestOpenBeforeElimination._1

      open -= bestOpenNodeID

      val existingVisitedOption =
        ExploreRunnerCache
          .get(bestOpenNodeID -> params.executionID)
          .reduceOption(visitedReducer)

      val bestOpen = existingVisitedOption match {
        case Some(allVisited) =>
          val dataRowsAfterElimination = pruneOpen(bestOpenBeforeElimination._2, allVisited)
          bestOpenBeforeElimination.copy(_2 = dataRowsAfterElimination)
        case None =>
          bestOpenBeforeElimination
      }
      bestOpen
    }
  }
}
