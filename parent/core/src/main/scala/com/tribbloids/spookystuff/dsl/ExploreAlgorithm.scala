package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.NodeKey
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

    /**
      */
    def openReducer: RowReducer

    def openReducerBetweenEpochs: RowReducer = openReducer

    def nextOpenSelector(
        open: ConcurrentMap[NodeKey, Iterable[DataRow]]
    ): (NodeKey, Iterable[DataRow])

    /**
      */
    def visitedReducer: RowReducer // precede eliminator

    def visitedReducerBetweenEpochs: RowReducer = visitedReducer
  }

  trait EliminatingImpl extends Impl {

    /**
      */
    val ordering: RowOrdering

    /**
      */
    def eliminator(
        open: Iterable[DataRow],
        visited: Iterable[DataRow]
    ): Iterable[DataRow]

    final override def nextOpenSelector(
        open: ConcurrentMap[NodeKey, Iterable[DataRow]]
    ): (NodeKey, Iterable[DataRow]) = {

      // Should I use pre-sorted collection? Or is it overengineering?
      val bestOpenBeforeElimination: (NodeKey, Iterable[DataRow]) = open.min(ordering)
      val bestOpenNodeID = bestOpenBeforeElimination._1

      open -= bestOpenNodeID

      val existingVisitedOption: Option[Iterable[DataRow]] =
        ExploreRunnerCache
          .get(bestOpenNodeID -> params.executionID)
          .reduceOption(visitedReducer)

      val bestOpen: (NodeKey, Iterable[DataRow]) = existingVisitedOption match {
        case Some(allVisited) =>
          val dataRowsAfterElimination = eliminator(bestOpenBeforeElimination._2, allVisited)
          bestOpenBeforeElimination.copy(_2 = dataRowsAfterElimination)
        case None =>
          bestOpenBeforeElimination
      }
      bestOpen
    }
  }
}
