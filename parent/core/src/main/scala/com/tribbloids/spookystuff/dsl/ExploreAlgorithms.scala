package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.NodeKey
import com.tribbloids.spookystuff.row._

import java.util.UUID

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

      override lazy val openReducer: RowReducer = { (v1, v2) =>
        val grouped: Map[Option[UUID], Iterable[DataRow]] = (v1 ++ v2)
          .groupBy(_.fastID)

        val result = if (grouped.isEmpty) {
          Nil
        } else {

          // rows that are discovered with deeper recursions are dropped
          grouped.values
            .minBy(v => v.head.sortIndex(Seq(depthField, ordinalField)))
        }

        result
      }

      override lazy val visitedReducer: RowReducer = openReducer

      override lazy val ordering: RowOrdering = Ordering.by { tuple: (NodeKey, Iterable[DataRow]) =>
        val inProgress = ExploreRunnerCache
          .getOnGoingRunners(params.executionID)
          .flatMap(_.fetchingInProgressOpt)

        val result = if (inProgress contains tuple._1) {
          Int.MaxValue
        } else {
          val v = tuple._2
          //          assert(v.size == 1)
          v.head
            .getInt(depthField)
            .getOrElse(Int.MaxValue)
        }
        result
      }

      override def eliminator(
          open: Iterable[DataRow],
          visited: Iterable[DataRow]
      ): Iterable[DataRow] = {
        val visitedDepth = visited.head.getInt(depthField)
        open.filter { row =>
          row.getInt(depthField) < visitedDepth
        }
      }
    }
  }

  // move reduce of openSet to elimination, should have identical result
  // case class ShortestPathImpl2(
  //                              depthField: IndexedField,
  //                              ordinalField: IndexedField,
  //                              extracts: Seq[Expression[Any]]
  //                            ) extends ExploreAlgorithmImpl {
  //
  //  import scala.Ordering.Implicits._
  //
  //  override def openReducer: RowReducer = {
  //    _ ++ _
  //  }
  //
  //  override def visitedReducer: RowReducer = {
  //    (v1, v2) =>
  //      Array((v1 ++ v2).minBy(_.sortIndex(Seq(depthField, ordinalField))))
  //  }
  //
  //  override def ordering: RowOrdering = Ordering.by{
  //    v: Iterable[DataRow] =>
  //      assert(v.size == 1)
  //      v.head.getInt(depthField).get
  //  }
  //
  //  override def eliminator: RowEliminator = {
  //    (v1, v2) =>
  //      assert(v2.size == 1)
  //      val visitedDepth = v2.head.getInt(depthField).get
  //      val filtered = v1.filter {
  //        row =>
  //          row.getInt(depthField).get < visitedDepth
  //      }
  //      if (filtered.isEmpty) filtered
  //      else Some(filtered.minBy(_.sortIndex(Seq(depthField, ordinalField))))
  //  }
  // }

  abstract class DepthFirst extends ExploreAlgorithm {

    override def getImpl(params: Params, schema: SpookySchema): Impl =
      Impl(params, schema)

    case class Impl(params: Params, schema: SpookySchema) extends EliminatingImpl {

      /**
        */
      override val ordering: RowOrdering = ???

      /**
        */
      override def eliminator(open: Iterable[DataRow], visited: Iterable[DataRow]): Iterable[DataRow] = ???

      /**
        */
      override val openReducer: RowReducer = ???

      /**
        */
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
