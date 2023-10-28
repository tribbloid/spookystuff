package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.NodeKey
import com.tribbloids.spookystuff.row._

object ExploreAlgorithms {

  import ExploreAlgorithm._

  case object BreadthFirst extends ExploreAlgorithm {

    override def getImpl(
        params: Params,
        schema: SpookySchema
    ): Impl = Impl(params, schema)

    case class Impl(
        override val params: Params,
        schema: SpookySchema
    ) extends EliminatingImpl {

      import params._

      import scala.Ordering.Implicits._

      private val _depth = schema(depth)
      private val _ordinal = schema(ordinal)

      override val openReducer: RowReducer = { (v1, v2) =>
        (v1 ++ v2)
          .groupBy(_.groupID)
          .values
          .minBy { v =>
            val first = v.head
            first.withFields(_depth, _ordinal).sortIndex
          }
      }

      override val visitedReducer: RowReducer = openReducer

      override val ordering: RowOrdering = Ordering.by { tuple: (NodeKey, Iterable[DataRow]) =>
        val inProgress = ExploreRunnerCache
          .getOnGoingRunners(params.executionID)
          .flatMap(_.fetchingInProgressOpt)

        val result = if (inProgress contains tuple._1) {
          Int.MaxValue
        } else {
          val v = tuple._2
          //          assert(v.size == 1)
          v.head
            .on(_depth)
            .int
            .getOrElse(Int.MaxValue)
        }
        result
      }

      override def eliminator(
          open: Iterable[DataRow],
          visited: Iterable[DataRow]
      ): Iterable[DataRow] = {
        val visitedDepth = visited.head.on(_depth).int
        open.filter { row =>
          row.on(_depth).int < visitedDepth
        }
      }
    }
  }

  // TODO: not sure about implementation
  abstract class DepthFirst extends ExploreAlgorithm {

    override def getImpl(params: Params, schema: SpookySchema): Impl =
      Impl(params, schema)

    case class Impl(params: Params, schema: SpookySchema) extends EliminatingImpl {
      override val ordering: RowOrdering = ???

      override def eliminator(open: Iterable[DataRow], visited: Iterable[DataRow]): Iterable[DataRow] = ???

      override val openReducer: RowReducer = ???

      override val visitedReducer: RowReducer = ???
    }
  }
}

//TODO: finish these
//case object AllSimplePath extends ExploreOptimizer

//case object AllPath extends ExploreOptimizer {
//  override def openReducer: RowReducer = {_ ++ _}
//
//  //precede eliminator
//  override def ordering: RowOrdering = {_ ++ _}
//
//  override def visitedReducer: RowReducer = {_ ++ _}
//
//  override def eliminator: RowEliminator = {(v1, v2) => v1}
//}

//this won't merge identical traces and do lookup, only used in case each resolve may yield different result
//case class Dijkstra(
//
//                   ) extends ExploreOptimizer {
//  override def openReducer: RowReducer = ???
//
//  //precede eliminator
//  override def ordering: RowOrdering = ???
//
//  override def visitedReducer: RowReducer = ???
//
//  override def eliminator: RowEliminator = ???
//}
