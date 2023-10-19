package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.dsl.ExploreAlgorithm
import com.tribbloids.spookystuff.execution.ExplorePlan.Open_Visited
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.Caching.ConcurrentMap
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import com.tribbloids.spookystuff.{dsl, SpookyContext}

import scala.language.implicitConversions

/**
  * NOT serializable: expected to be constructed on Executors
  */
class ExploreRunner(
    val itr: Iterator[(TraceView, Open_Visited)], // TODO: change to TraceView,
    val impl: ExploreAlgorithm.Impl,
    val keyBy: Trace => Any
) extends NOTSerializable {

  import dsl._

  // TODO: add fast sort implementation
  val open: ConcurrentMap[NodeKey, Iterable[DataRow]] =
    ConcurrentMap() // TODO: Change to ConcurrentMap[TraceView, Array[DataRow]]

  //  val openVs = mutable.SortedSet[(TraceView, Array[DataRow])] = mutable.SortedSet()
  val visited: ConcurrentMap[NodeKey, Iterable[DataRow]] = ConcurrentMap()

  @volatile var fetchingInProgressOpt: Option[NodeKey] = None

  itr.foreach { tuple =>
    tuple._2.open.map(v => open += tuple._1.keyBy(keyBy) -> v)
    tuple._2.visited.map(v => visited += tuple._1.keyBy(keyBy) -> v)
  }

  protected def commitIntoVisited(
      key: NodeKey,
      value: Iterable[DataRow],
      reducer: RowReducer
  ): Unit = {
    val oldVs: Iterable[DataRow] = visited.getOrElse(key, Nil)
    val newVs = reducer(value, oldVs)
    visited.put(key, newVs)
  }

  protected def executeOnce(
      resolved: Resolved[Any],
      sampler: Sampler[Any],
      joinType: JoinType,
      trace: Set[Trace]
  )(
      `depth_++`: Resolved[Int],
      spooky: SpookyContext
  )(
      rowFn: SquashedFetchedRow => SquashedFetchedRow
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Unit = {

    import impl._
    import params._

    implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WSchema =
      row.WSchema(schema)

    val bestOpen: (NodeKey, Iterable[DataRow]) = nextOpenSelector(open)

    if (bestOpen._2.nonEmpty) {
      this.fetchingInProgressOpt = Some(bestOpen._1)

      val bestRow_- = SquashedFetchedRow(bestOpen._2.toArray, bestOpen._1)

      val bestRow = rowFn.apply(
        bestRow_-
          .extract(depth_++)
      )
      val bestDataRowsInRange = bestRow.dataRows.filter { dataRow =>
        range.contains(dataRow.getInt(depth_++.field).get)
      }

      this.commitIntoVisited(bestOpen._1, bestDataRowsInRange, visitedReducer)

      val bestNonFringeRow = bestRow.copy(
        dataRows = bestRow.dataRows.filter { dataRow =>
          dataRow.getInt(depth_++.field).get < range.max
        }
      )

      val newOpens: Array[(TraceView, DataRow)] = bestNonFringeRow
        .extract(resolved)
        .flattenData(resolved.field, ordinalField, joinType.isLeft, sampler)
        .interpolateAndRewriteLocally(trace)
        .map { tuple =>
          tuple._1 -> tuple._2
        }
      newOpens.foreach { newOpen =>
        val traceView_+ = newOpen._1
        val node_+ = traceView_+.keyBy(ExploreRunner.this.keyBy)
        val oldDataRows: Iterable[DataRow] = open.getOrElse(node_+, Nil)
        val newDataRows = openReducer(Array(newOpen._2), oldDataRows).toArray
        open += node_+ -> newDataRows
      }
      this.fetchingInProgressOpt = None
    }
  }

  // TODO: need unit test if this preserve keyBy
  def run(
      resolved: Resolved[Any],
      sampler: Sampler[Any],
      joinType: JoinType,
      traces: Set[Trace]
  )(
      maxItr: Int,
      `depth_++`: Resolved[Int],
      spooky: SpookyContext
  )(
      rowMapper: SquashedFetchedRow => SquashedFetchedRow
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Iterator[(TraceView, Open_Visited)] =
    try {

      ExploreRunnerCache.register(this, impl.params.executionID)

      // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
      // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times.
      // Does this mess with the reducer?
      def finish(): Iterator[(TraceView, Open_Visited)] = {
        val open = this.open
          .map(t => t._1 -> Open_Visited(open = Some(t._2.toArray)))
        val visited = this.visited
          .map(t => t._1 -> Open_Visited(visited = Some(t._2.toArray)))

        val toBeCommitted = this.visited
          .map { tuple =>
            (tuple._1 -> impl.params.executionID) -> tuple._2
          }

        ExploreRunnerCache.commit(toBeCommitted, impl.visitedReducer)

        this.finalize()

        (open ++ visited).map {
          case (node, open_visit) =>
            node -> open_visit
        }.iterator
      }

      for (_ <- 0 to maxItr) {
        if (open.isEmpty) return finish()
        executeOnce(resolved, sampler, joinType, traces)(`depth_++`, spooky)(rowMapper)
      }

      finish()
    } finally {
      ExploreRunnerCache.deregister(this, impl.params.executionID)
    }
}
