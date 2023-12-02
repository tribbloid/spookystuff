package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Action, Trace, TraceSet}
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.dsl
import com.tribbloids.spookystuff.dsl.ExploreAlgorithm
import com.tribbloids.spookystuff.execution.ExplorePlan.Open_Visited
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.Caching.ConcurrentMap
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

import scala.language.implicitConversions

/**
  * NOT serializable: expected to be constructed on Executors
  */
class ExploreAlgorithmRunner(
    val itr: Iterator[(Trace, Open_Visited)],
    val impl: ExploreAlgorithm.Impl,
    keyBy: List[Action] => Any
) extends NOTSerializable {

  import dsl._

  // TODO: add fast sort implementation
  val open: ConcurrentMap[Trace, Vector[DataRow]] =
    ConcurrentMap() // TODO: Change to ConcurrentMap[TraceView, Vector[DataRow]]

  //  val openVs = mutable.SortedSet[(TraceView, Array[DataRow])] = mutable.SortedSet()
  val visited: ConcurrentMap[Trace, Vector[DataRow]] = ConcurrentMap()

  @volatile var fetchingInProgressOpt: Option[Trace] = None

  itr.foreach { tuple =>
    tuple._2.open.map(v => open += tuple._1.setSamenessFn(keyBy) -> v)
    tuple._2.visited.map(v => visited += tuple._1.setSamenessFn(keyBy) -> v)
  }

  protected def commitIntoVisited(
      key: Trace,
      value: Vector[DataRow],
      reducer: RowReducer
  ): Unit = {
    val oldVs: Vector[DataRow] = visited.getOrElse(key, Vector.empty)
    val newVs = reducer(value, oldVs)
    visited.put(key, newVs)
  }

  protected def executeOnce(
      resolved: Resolved[Any],
      sampler: Sampler[Any],
      forkType: ForkType,
      traces: TraceSet
  )(
      `depth_++`: Resolved[Int]
  )(
      rowFn: BottleneckRow => BottleneckRow
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Unit = {

    import impl.params._

    implicit def wSchema(row: BottleneckRow): BottleneckRow#WSchema =
      row.WSchema(impl.schema)

    val bestOpen: (Trace, Vector[DataRow]) = impl.selectNext(open)

    if (bestOpen._2.nonEmpty) {
      this.fetchingInProgressOpt = Some(bestOpen._1)

      val bestRow_- = BottleneckRow(bestOpen._2, bestOpen._1)

      val bestRow = rowFn.apply(
        bestRow_-
          .extract(depth_++)
      )

      val maxRange = range.max

      val bestDataRowsInRange = bestRow.dataRows.filter { dataRow =>
        dataRow.getInt(depth_++.field).get < maxRange
      }

      this.commitIntoVisited(bestOpen._1, bestDataRowsInRange, impl.visitedReducer)

      val bestRowInRange = bestRow.copy(
        dataRows = bestRow.dataRows.filter { dataRow =>
          dataRow.getInt(depth_++.field).get < maxRange
        }
      )

      val newOpens = {

        val rewritten = bestRowInRange
          .extract(resolved)
          .explodeData(resolved.field, ordinalField, forkType, sampler)
          .interpolateAndRewrite(traces)

        val grouped = rewritten
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2))

        grouped
      }

      newOpens.foreach { newOpen =>
        val traceView_+ = newOpen._1
        val nodeID_+ = traceView_+.setSamenessFn(ExploreAlgorithmRunner.this.keyBy)
        val oldDataRows: Vector[DataRow] = open.getOrElse(nodeID_+, Vector.empty)
        val newDataRows = impl.openReducer(newOpen._2.toVector, oldDataRows)
        open += nodeID_+ -> newDataRows
      }
      this.fetchingInProgressOpt = None
    }
  }

  // TODO: need unit test if this preserve keyBy
  def run(
      resolved: Resolved[Any],
      sampler: Sampler[Any],
      forkType: ForkType,
      traces: TraceSet
  )(
      maxItr: Int,
      `depth_++`: Resolved[Int]
  )(
      rowMapper: BottleneckRow => BottleneckRow
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Iterator[(Trace, Open_Visited)] =
    try {

      ExploreRunnerCache.register(this, impl.params.executionID)

      // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
      // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times.
      // Does this mess with the reducer?
      def finish(): Iterator[(Trace, Open_Visited)] = {
        val open = this.open
          .map(t => t._1 -> Open_Visited(open = Some(t._2)))
        val visited = this.visited
          .map(t => t._1 -> Open_Visited(visited = Some(t._2)))

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
        executeOnce(resolved, sampler, forkType, traces)(`depth_++`)(rowMapper)
      }

      finish()
    } finally {
      ExploreRunnerCache.deregister(this, impl.params.executionID)
    }
}
