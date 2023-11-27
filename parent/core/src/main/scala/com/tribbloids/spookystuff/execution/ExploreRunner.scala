package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.PathPlanning
import com.tribbloids.spookystuff.execution.ExplorePlan.{ExeID, State}
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import com.tribbloids.spookystuff.{dsl, SpookyContext}

import scala.collection.{mutable, MapView}

/**
  * NOT serializable: expected to be constructed on Executors
  */
case class ExploreRunner(
    partition: Iterator[(LocalityGroup, State)],
    pathPlanningImpl: PathPlanning.Impl,
    sameBy: Trace => Any,
    depth0: Resolved[Int],
    `depth_++`: Resolved[Int]
) extends NOTSerializable {

  import dsl._

  def exeID: ExeID = pathPlanningImpl.params.executionID

  lazy val spooky: SpookyContext = pathPlanningImpl.schema.spooky

  // TODO: add fast sorted implementation
  val open: ConcurrentMap[LocalityGroup, Vector[DataRow]] =
    ConcurrentMap()

  val visited: ConcurrentMap[LocalityGroup, Vector[DataRow]] = ConcurrentMap()

  lazy val row0Partition: Iterator[SquashedRow] = {
    partition.flatMap {
      case (group, state) =>
        val _group = group.sameBy(sameBy)

//        state.row0.map { v =>
//          row0s += v.copy(agentState = v.agentState.copy(group = _group))
//        }
        state.open.foreach { v =>
          open += _group -> v
        }
        state.visited.foreach { v =>
          visited += _group -> v
        }

        state.row0.map { v =>
          v.copy(agentState = v.agentState.copy(group = _group))
        }
    }
  }

  def isFullyExplored: Boolean = row0Partition.isEmpty && open.isEmpty

  @volatile var fetchingInProgressOpt: Option[LocalityGroup] = None

  private case class Commit(
      group: LocalityGroup,
      value: Vector[DataRow]
  ) {

    def intoOpen(
        reducer: DataRow.Reducer = pathPlanningImpl.openReducer
    ): mutable.Map[LocalityGroup, Vector[DataRow]] = {
      val oldVs: Vector[DataRow] = open.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      open += group -> newVs
    }

    def intoVisited(
        reducer: DataRow.Reducer = pathPlanningImpl.visitedReducer
    ): mutable.Map[LocalityGroup, Vector[DataRow]] = {
      val oldVs: Vector[DataRow] = visited.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      visited += group -> newVs
    }
  }

  private def selectNext(): SquashedRow = {

    val selectedRow: SquashedRow = {
      val row = row0Partition
        .nextOption()
        .map { row =>
          row
            .withCtx(spooky)
            .extract(depth0)
        }
        .getOrElse {

          val selected: (LocalityGroup, Vector[DataRow]) = pathPlanningImpl.selectNextOpen(open)
          val withDepth = SquashedRow(AgentState(selected._1), selected._2.map(_.withEmptyScope))
            .withCtx(spooky)
            .resetScope
            .withCtx(spooky)
            .extract(depth_++)
            .withLineageIDs

          //          val transformed = delta.fn(withDepth)
          withDepth
        }
      row
    }

//    println(
//      s"investigating: ${FilePaths.Hierarchical(selectedRow.agentState.group.trace)}, ${row0Partition.hasNext} + ${open.size} left"
//    )

    selectedRow
  }

  protected def executeOnce(
      forkExpr: Resolved[Any],
      sampler: Sampler[Any],
      forkType: ForkType,
      traces: TraceSet
  )(
      delta: Delta
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Unit = {

    import pathPlanningImpl.params._

    val selectedRow = selectNext()

    if (selectedRow.dataRows.isEmpty) return

    this.fetchingInProgressOpt = Some(selectedRow.group)

    val transformed = delta.fn(selectedRow)

    {
      // commit transformed data into visited
      val data = transformed.dataRows.map(_.self).toVector

      Commit(transformed.group, data).intoVisited()
    }

    {
      val forked = transformed
        .withCtx(spooky)
        .extract(forkExpr)
        .explodeData(forkExpr.field, ordinalField, forkType, sampler)

      val interpolated: Seq[(Trace, DataRow)] = forked
        .withSchema(pathPlanningImpl.schema)
        .interpolateAndRewrite(traces)
        .filter {
          case (trace, _) => trace.nonEmpty // empty trace indicates failed interpolation
        }
//        .map {
//          case (trace, dataRow) =>
//            trace -> dataRow.--(Seq(forkExpr.field)) // remove forkExpr.field from dataRow
//        }

      val grouped: MapView[LocalityGroup, Seq[DataRow]] = interpolated
        .groupBy(v => LocalityGroup(v._1)().sameBy(sameBy))
        .view
        .mapValues(_.map(_._2))

      val maxRange = effectiveRange.max // TODO: use effectiveRange.min
      // this will be used to filter dataRows yield by the next fork, it will not affect current transformation

      val filtered = grouped
        .mapValues { v =>
          val inRange = v.filter { dataRow =>
            val depth = dataRow.getInt(depthField).getOrElse(Int.MaxValue)
            depth < maxRange - 1
          }
          inRange
        }
        .filter {
          case (_, v) =>
            v.nonEmpty
        }
        .toList

      filtered.foreach { newOpen =>
        val trace_+ = newOpen._1
        Commit(trace_+, newOpen._2.toVector).intoOpen()
      }

    }

    this.fetchingInProgressOpt = None
  }

  def run(
      expr: Resolved[Any],
      sampler: Sampler[Any],
      forkType: ForkType,
      traces: TraceSet
  )(
      maxItr: Int,
      delta: Delta
      // apply immediately after depth selection, this include depth0
      // should include flatten & extract
  ): Iterator[(LocalityGroup, State)] =
    try {

      ExploreLocalCache.register(this)

      // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
      // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times.
      // Does this mess with the reducer?
      def finish(): Iterator[(LocalityGroup, State)] = {
        val allKeys = this.open.keySet ++ this.visited.keySet

        val result = allKeys.map { key =>
          val open = this.open.get(key)
          val visited = this.visited.get(key)

          val state = State(
            None,
            open,
            visited
          )
          key -> state
        }

        ExploreLocalCache.commitVisited(this, pathPlanningImpl.visitedReducer)
        // committed rows can no longer be canceled, only evicted by soft cache

        result.iterator
      }

      while (row0Partition.hasNext) {
        executeOnce(expr, sampler, forkType, traces)(delta)
      }
      // row0 has to be fully executed

      for (_ <- 0 to maxItr) {
        if (isFullyExplored) return finish()
        executeOnce(expr, sampler, forkType, traces)(delta)
      }

      finish()
    } finally {
      ExploreLocalCache.deregister(this)
    }
}
