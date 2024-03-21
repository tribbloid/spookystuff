package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row._

import scala.collection.mutable

object PathPlanners_Simple {

  import PathPlanning._

  case object BreadthFirst extends PathPlanning {

    case class _Impl[D](
        override val params: Params,
        schema: SpookySchema[D]
    ) extends Impl.CanPruneSelected[D] {

      import scala.Ordering.Implicits._

      override val openReducer: Reducer = { (v1, v2) =>
        val map = {
          (v1 ++ v2).groupBy { v =>
            v.lineageID.get
          }
        }

        val candidates: Seq[Vector[Lineage]] = map.values.toSeq

        if (candidates.isEmpty) Vector.empty
        else if (candidates.size == 1) candidates.head
        else {

          val result = candidates
            .minBy { v =>
              val data = v.map(_.data)

              val minData = data.min(schema.ordering)
              // TODO: this may need validation, not sure if consistent with old impl
              minData
            }(schema.ordering)

          result
        }
      }

      override val visitedReducer: Reducer = openReducer

      override val ordering: RowOrdering = Ordering.by { tuple: (LocalityGroup, Vector[Lineage]) =>
        val inProgress: mutable.Set[LocalityGroup] = ExploreLocalCache
          .getOnGoingRunners(params.executionID)
          .flatMap(_.fetchingInProgressOpt)

        val result: (Int, Seq[List[Int]]) = if (inProgress contains tuple._1) {
          Int.MaxValue -> Vector.empty
          // if in progress by any local executor, do not select, wait for another executor to finish it first
        } else {
          val dataRows = tuple._2
          val firstDataRow = dataRows.head

          val sortEvs = firstDataRow.sortIndex(schema.sortIndices: _*)
          0 -> sortEvs
        }
        result
      }

      override protected def pruneSelectedNonEmpty(
          open: Vector[Lineage],
          visited: Vector[Lineage]
      ): Vector[Lineage] = {

        val visitedDepth = visited.head.getInt(depthField)
        open.filter { row =>
          row.getInt(depthField) < visitedDepth
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
