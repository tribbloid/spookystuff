package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.{Explore, ExploreRunner}
import com.tribbloids.spookystuff.row._

import scala.collection.mutable

object PathPlanners_Simple {

  import PathPlanning._

  case object BreadthFirst extends PathPlanning {

    override def _Impl[I, O](params: Params, schema: SpookySchema): _Impl[I, O] = {
      new _Impl(params, schema) // why is it necessary?
    }
    case class _Impl[I, O](
        override val params: Params,
        schema: SpookySchema
    ) extends Impl.CanPruneSelected[I, O] {

      import scala.Ordering.Implicits._

      class ReducerProto[T] extends Explore.ReducerK[T] {
        override def reduce(v1: Elems, v2: Elems): Elems = {

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
                val sortEv = vs.map(_.sortEv).min
                // TODO: this may need validation, not sure if consistent with old impl
                sortEv
              }

            result
          }
        }
      }

      override val openReducer: OpenReducer = {
        new ReducerProto[I]
      }

      override val visitedReducer: VisitedReducer = {

        new ReducerProto[O]
      }

      override val ordering: RowOrdering = Ordering.by { tuple: (LocalityGroup, Vector[Elem]) =>
        val inProgress: mutable.Set[LocalityGroup] = ExploreLocalCache
          .getOnGoingRunners(params.executionID)
          .flatMap { v: ExploreRunner[I, O] =>
            v.fetchingInProgressOpt
          }

        val result = if (inProgress contains tuple._1) {
          (Int.MaxValue, None, Vector.empty)
          // if in progress by any local executor, do not select, wait for another executor to finish it first
        } else {
          val dataRows = tuple._2
          val firstDataRow: Elem = dataRows.head

          (0, firstDataRow.depthOpt, firstDataRow.ordinal)
        }

        result
      }

      override protected def pruneSelectedNonEmpty(
          open: Elems,
          visited: Outs
      ): Vector[Elem] = {

        val visitedDepth = visited.head.depthOpt
        open.filter { row =>
          row.depthOpt < visitedDepth
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
