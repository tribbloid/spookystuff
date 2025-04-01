package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.PathPlanning
import com.tribbloids.spookystuff.execution.ExplorePlan.{ExeID, State}
import com.tribbloids.spookystuff.execution.FetchPlan.Batch
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
  val open: ConcurrentMap[LocalityGroup, Open.Batch] =
    ConcurrentMap()

  val visited: ConcurrentMap[LocalityGroup, Visited.Batch] = ConcurrentMap()

  lazy val row0Partition: BufferedIterator[SquashedRow[I]] = {
    val unbuffered: Iterator[SquashedRow[I]] = partition.flatMap {
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
    val buffered = unbuffered.buffered // TODO: are there more memory-light buffer with length 1?
    buffered
  }

  def isFullyExplored: Boolean = row0Partition.isEmpty && open.isEmpty

  @volatile var fetchingInProgressOpt: Option[LocalityGroup] = None

  private case class Commit(
      group: LocalityGroup
  ) {

    def isNoOp: Boolean = group.trace.trace.isEmpty

    def filterByMaxDepth[T](batch: Explore.BatchK[T]): Explore.BatchK[T] = {
      val result = batch.flatMap { v =>
        val depth = v._1.depth

        val reprOpt: Option[Data.Exploring[T]] = if (depth < depthRange.max) {
          Some(v._1)
        } else {
          None
        }

        reprOpt.map { next =>
          next -> v._2
        }
      }
      result
    }

    def intoOpen(
        value: Open.Batch,
        reducer: Open.Reducer = pathPlanningImpl.openReducer
    ): Unit = {
      if (isNoOp) return // NoOp will terminate the ExploreRunner

      val effective = filterByMaxDepth(value)

      val oldVs: Open.Batch = open.getOrElse(group, Vector.empty)
      val newVs = reducer(effective, oldVs)
      open += group -> newVs

      open
    }

    def intoVisited(
        value: Visited.Batch,
        reducer: Visited.Reducer = pathPlanningImpl.visitedReducer
    ): Unit = {

      val effective = filterByMaxDepth(value)
      // TODO: no need to mark, just delete immediately
      val markedByMinDepth = effective.map { v =>
        val depth = v._1.depth

        val result = if (depth < depthRange.min) {
          v._1.copy(isOutOfRange = true) -> v._2
        } else {
          v
        }
        result
      }

      val oldVs: Visited.Batch = visited.getOrElse(group, Vector.empty)
      val newVs = reducer(markedByMinDepth, oldVs)
      visited += group -> newVs

//      val __DEBUG = newVs.map(v => v._1.depth)

      visited
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

      val unSquashedRows: Seq[AgentRow[Open.Exploring]] = selectedRow.withCtx(ctx).unSquash

      val _ = unSquashedRows.map { input =>
        val openExploring: Open.Exploring = input.data

        val (_induction: Batch[I], _outs: FlatMapPlan.Batch[O]) = fn(input)

        {

          val visited = _outs.map { v =>
            openExploring.copy(v) -> input.index
          }

          // commit out into visited
//          val inRange: Visited.Batch = _outs.flatMap { out =>
//            val result: Data.Exploring[O] = openExploring.copy(raw = out)
//
//            val depth = result.depth
//
//            val max = maxRange
//            val min = minRange
//
//            val reprOpt: Option[(Data.Exploring[O], Int)] = if (depth < minRange) {
//              Some(result.copy(isOutOfRange = true) -> input.index)
//            } else if (depth < maxRange) {
//              Some(result -> input.index)
//            } else {
//              None
//            }
//
//            reprOpt
//          }

          Commit(selectedRow.localityGroup).intoVisited(visited)
        }

        {
          // recursively generate new openSet
          val fetched: Seq[(Trace, (Open.Exploring, Int))] = _induction.zipWithIndex.flatMap {
            case ((nexTtraceSet, nextData), index) =>
              val nextElem: Open.Exploring = openExploring.depth_++.copy(nextData)

              nexTtraceSet.traceSet.map { trace =>
                trace -> (nextElem -> index)
              }
          }

          val grouped: MapView[LocalityGroup, Open.Batch] = fetched
            .groupBy(v => LocalityGroup(v._1).sameBy(sameByFn))
            .view
            .mapValues(_.map(_._2))

          // this will be used to filter dataRows yield by the next fork, it will not affect current transformation
          val filtered: List[(LocalityGroup, Open.Batch)] = grouped.filter {
            case (_, v) =>
              v.nonEmpty
          }.toList

          filtered.foreach { (newOpen: (LocalityGroup, Open.Batch)) =>
            val trace_+ = newOpen._1
            Commit(trace_+).intoOpen(newOpen._2.toVector)
          }
        }
      }

      outer.fetchingInProgressOpt = None
    }

    def recursively(
        maxItr: Int
    ): collection.Set[(LocalityGroup, State[I, O])] =
      try {

        ExploreLocalCache.register(outer)

        // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
        // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times.
        // Does this mess with the reducer?
        def finish() = {

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

          result
        }

        while (row0Partition.headOption.nonEmpty) {
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
