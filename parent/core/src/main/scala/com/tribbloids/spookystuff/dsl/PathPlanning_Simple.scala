package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.{Explore, ExploreRunner}
import com.tribbloids.spookystuff.row.*

import scala.collection.mutable

object PathPlanning_Simple {

  import PathPlanning.*

  case object BreadthFirst extends PathPlanning {

    override def _Impl[I, O](params: Params, schema: SpookySchema): _Impl[I, O] = {
      new _Impl(params, schema) // why is it necessary?
    }
    case class _Impl[I, O](
        override val params: Params,
        schema: SpookySchema
    ) extends Impl.CanPruneSelected[I, O] {

      import scala.Ordering.Implicits.*

      class ReducerProto[T] extends Explore.ReducerK[T] {
        override def reduce(v1: Batch, v2: Batch): Batch = {

          val map = {
            (v1 ++ v2).groupBy { v =>
              v.lineageID.get
            }
          }

          val candidates = map.values.toSeq

          if (candidates.isEmpty) Vector.empty
          else if (candidates.size == 1) candidates.head
          else {

            val result = candidates
              .minBy { vs =>
                val sortEv = vs.map(_.orderBy).min
                // TODO: this may need validation, not sure if consistent with old impl
                sortEv
              }

            result
          }
        }
      }

      override val openReducer: Open.Reducer = {
        new ReducerProto[I]
      }

      override val visitedReducer: Visited.Reducer = {

        new ReducerProto[O]
      }

      override val ordering: Open.RowOrdering = Ordering.by { (tuple: (LocalityGroup, Seq[Open.Exploring])) =>
        val inProgress: mutable.Set[LocalityGroup] = ExploreLocalCache
          .getOnGoingRunners(params.executionID)
          .flatMap { (v: ExploreRunner[I, O]) =>
            v.fetchingInProgressOpt
          }

        val result: (Int, (Int, Vector[Int])) = if (inProgress contains tuple._1) {
          (Int.MaxValue, (Int.MaxValue, Vector.empty))
          // if in progress by any local executor, do not select, wait for another executor to finish it first
        } else {
          val dataRows = tuple._2
          val firstDataRow = dataRows.head

          (0, firstDataRow.orderBy)
        }

        result
      }

      override protected def pruneSelectedNonEmpty(
          open: Open.Batch,
          visited: Visited.Batch
      ): Seq[Open.Exploring] = {

        val visitedDepth = visited.head.depth
        open.filter { row =>
          row.depth < visitedDepth
        }
      }
    }

  }

//  abstract class DepthFirst extends PathPlanning {}
//
//  abstract class Dijkstra extends PathPlanning {}
//
//  abstract class AStar extends PathPlanning {}
}
