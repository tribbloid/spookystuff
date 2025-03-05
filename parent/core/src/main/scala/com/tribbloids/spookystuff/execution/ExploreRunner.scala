package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.PathPlanning
import com.tribbloids.spookystuff.execution.ExplorePlan.{ExeID, State}
import com.tribbloids.spookystuff.row.*

import scala.collection.MapView

object ExploreRunner {}

/**
  * NOT serializable: expected to be constructed on Executors
  */
case class ExploreRunner[I, O](
    partition: Iterator[(LocalityGroup, State[I, O])],
    pathPlanningImpl: PathPlanning.Impl[I, O],
    sameByFn: Trace => Any
) extends Explore.Common[I, O]
    with NOTSerializable {

  import pathPlanningImpl.params.*

  def exeID: ExeID = pathPlanningImpl.params.executionID

  lazy val ctx: SpookyContext = pathPlanningImpl.schema.ctx

  // TODO: add fast sorted implementation
  val open: ConcurrentMap[LocalityGroup, Seq[Open.Exploring]] =
    ConcurrentMap()

  val visited: ConcurrentMap[LocalityGroup, Visited.Batch] = ConcurrentMap()

  lazy val row0Partition: Iterator[SquashedRow[I]] = {
    partition.flatMap {
      case (group, state) =>
        val _group = group.sameBy(sameByFn) // old group key override will be ignored

//        state.row0.map { v =>
//          row0s += v.copy(agentState = v.agentState.copy(group = _group))
//        }
        state.open.foreach { v =>
          open += _group -> v
        }
        state.visited.foreach { v =>
          visited += _group -> v.toVector
        }

        state.row0.map { v =>
          v.copy(localityGroup = _group)
        }
    }
  }

  def isFullyExplored: Boolean = row0Partition.isEmpty && open.isEmpty

  @volatile var fetchingInProgressOpt: Option[LocalityGroup] = None

  private case class Commit(
      group: LocalityGroup
  ) {

    def isNoOp: Boolean = group.trace.trace.isEmpty

    def intoOpen(
        value: Seq[Open.Exploring],
        reducer: Open.Reducer = pathPlanningImpl.openReducer
    ): Unit = {
      if (isNoOp) return // NoOp will terminate the ExploreRunner

      val oldVs: Seq[Open.Exploring] = open.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      open += group -> newVs
    }

    def intoVisited(
        value: Vector[Visited.Exploring],
        reducer: Visited.Reducer = pathPlanningImpl.visitedReducer
    ): Unit = {
      val oldVs: Seq[Visited.Exploring] = visited.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      visited += group -> newVs
    }
  }

  def commitVisitedIntoLocalCache(
      reducer: Visited.Reducer
  ): Unit = {

    // TODO relax synchronized check to accelerate?
    def commit1(
        key: (LocalityGroup, ExeID),
        value: Visited.Batch
    ): Unit = {

      val exe = ExploreLocalCache.getExecution[I, O](key._2)

      exe.visited.synchronized {
        val oldVs: Option[Visited.Batch] = exe.visited.get(key._1)
        val newVs = (Seq(value) ++ oldVs).reduce(reducer)
        exe.visited.put(key._1, newVs.toVector)
      }
    }

    val toBeCommitted = this.visited
      .map { tuple =>
        (tuple._1 -> this.pathPlanningImpl.params.executionID) -> tuple._2
      }

    toBeCommitted.foreach { kv =>
      commit1(kv._1, kv._2)
    }
  }

  private def selectNext(): SquashedRow[Open.Exploring] = {

    val selectedRow: SquashedRow[Open.Exploring] = {
      val row0Opt: Option[SquashedRow[Open.Exploring]] = row0Partition
        .nextOption()
        .map { row =>
          row.exploring.startLineage
        }

      val row: SquashedRow[Open.Exploring] = row0Opt
        .getOrElse {

          val selected: (LocalityGroup, Open.Batch) = pathPlanningImpl.selectNextOpen(open)
          val withLineage: SquashedRow[Open.Exploring] =
            SquashedRow(selected._1, selected._2)

          //          val transformed = delta.fn(withDepth)
          withLineage
        }

      row
    }

//    println(
//      s"investigating: ${FilePaths.Hierarchical(selectedRow.agentState.group.trace)}, ${row0Partition.hasNext} + ${open.size} left"
//    )

    selectedRow
  }

  case class Run(
      fn: _Fn
  ) {

    private def outer = ExploreRunner.this

    def once(): Unit = {

      val selectedRow: SquashedRow[Open.Exploring] = selectNext()

      if (selectedRow.batch.isEmpty) return

      outer.fetchingInProgressOpt = Some(selectedRow.localityGroup)

      val unSquashedRows: Seq[FetchedRow[Open.Exploring]] = selectedRow.withCtx(ctx).unSquash

      val _ = unSquashedRows.map { input =>
        val openExploring: Open.Exploring = input.data

        val (_induction, _outs) = fn(input)

        {
          // commit out into visited
          val inRange: Explore.BatchK[O] = _outs.flatMap { out =>
            val result = openExploring.copy(raw = out)

            val depth = result.depth

            val reprOpt = if (depth < minRange) {
              Some(result.copy(isOutOfRange = true))
            } else if (depth < maxRange) {
              Some(result)
            } else {
              None
            }

            reprOpt
//            reprOpt.map { e =>
//              openExploring.copy(raw = e)
//            }
          }

          Commit(selectedRow.localityGroup).intoVisited(inRange.toVector)
        }

        {
          // recursively generate new openSet
          val fetched: Seq[(Trace, Open.Exploring)] = _induction.flatMap {
            case (nexTtraceSet, nextData) =>
              val nextElem: Open.Exploring = openExploring.depth_++.copy(nextData)

//              val nextElem: Open.Exploring = openExploring.copy(raw = nextElem)
              nexTtraceSet.traceSet.map { trace =>
                trace -> nextElem
              }
          }

          val grouped: MapView[LocalityGroup, Seq[Open.Exploring]] = fetched
            .groupBy(v => LocalityGroup(v._1).sameBy(sameByFn))
            .view
            .mapValues(_.map(_._2))

          // this will be used to filter dataRows yield by the next fork, it will not affect current transformation
          val filtered: List[(LocalityGroup, Seq[Open.Exploring])] = grouped.filter {
            case (_, v) =>
              v.nonEmpty
          }.toList

          filtered.foreach { (newOpen: (LocalityGroup, Seq[Open.Exploring])) =>
            val trace_+ = newOpen._1
            Commit(trace_+).intoOpen(newOpen._2.toVector)
          }
        }
      }

      outer.fetchingInProgressOpt = None
    }

    def recursively(
        maxItr: Int
    ): Iterator[(LocalityGroup, State[I, O])] =
      try {

        ExploreLocalCache.register(outer)

        // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
        // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times.
        // Does this mess with the reducer?
        def finish(): Iterator[(LocalityGroup, State[I, O])] = {

          val allKeys = outer.open.keySet ++ outer.visited.keySet

          val result = allKeys.map { key =>
            val open = outer.open.get(key)
            val visited = outer.visited.get(key)

            val state = State(
              None,
              open,
              visited
            )
            key -> state
          }

          outer.commitVisitedIntoLocalCache(pathPlanningImpl.visitedReducer)
          // committed rows can no longer be canceled, only evicted by soft cache

          result.iterator
        }

        while (row0Partition.hasNext) {
          once()
        }
        // row0 has to be fully executed

        for (_ <- 0 to maxItr) {
          if (isFullyExplored) return finish()
          once()
        }

        finish()
      } finally {
        ExploreLocalCache.deregister(outer)
      }
  }
}
