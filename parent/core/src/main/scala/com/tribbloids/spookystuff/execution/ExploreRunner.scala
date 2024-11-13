package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.dsl.PathPlanning
import com.tribbloids.spookystuff.execution.ExplorePlan.{ExeID, State}
import com.tribbloids.spookystuff.row.*

import scala.collection.{mutable, MapView}

object ExploreRunner {}

/**
  * NOT serializable: expected to be constructed on Executors
  */
case class ExploreRunner[I, O](
    partition: Iterator[(LocalityGroup, State[I, O])],
    pathPlanningImpl: PathPlanning.Impl[I, O],
    sameBy: Trace => Any
) extends Explore.Common[I, O]
    with NOTSerializable {

  import pathPlanningImpl.params.*

  def exeID: ExeID = pathPlanningImpl.params.executionID

  lazy val spooky: SpookyContext = pathPlanningImpl.schema.ctx

  // TODO: add fast sorted implementation
  val open: ConcurrentMap[LocalityGroup, Vector[Open.Payload]] =
    ConcurrentMap()

  val visited: ConcurrentMap[LocalityGroup, Visited.Batch] = ConcurrentMap()

  lazy val row0Partition: Iterator[SquashedRow[I]] = {
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
          v.copy(localityGroup = _group)
        }
    }
  }

  def isFullyExplored: Boolean = row0Partition.isEmpty && open.isEmpty

  @volatile var fetchingInProgressOpt: Option[LocalityGroup] = None

  private case class Commit(
      group: LocalityGroup
  ) {

    def intoOpen(
        value: Vector[Open.Payload],
        reducer: Open.Reducer = pathPlanningImpl.openReducer
    ): Unit = {
      val oldVs: Vector[Open.Payload] = open.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      open += group -> newVs
    }

    def intoVisited(
        value: Vector[Visited.Payload],
        reducer: Visited.Reducer = pathPlanningImpl.visitedReducer
    ): Unit = {
      val oldVs: Vector[Visited.Payload] = visited.getOrElse(group, Vector.empty)
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
        exe.visited.put(key._1, newVs)
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
          row.exploring
        }

      val row: SquashedRow[Open.Exploring] = row0Opt
        .getOrElse {

          val selected: (LocalityGroup, Open.Batch) = pathPlanningImpl.selectNextOpen(open)
          val withLineage: SquashedRow[Open.Exploring] =
            SquashedRow(selected._1, selected._2.map(v => Data.Scoped.default(v)))

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

      val unSquashedRows: Seq[FetchedRow[Open.Exploring]] = selectedRow.withCtx(spooky).unSquash

      val _ = unSquashedRows.map { input =>
        val openExploring: Open.Payload = input.payload

        val (_induction, _outs) = fn(input)

        {
          // commit out into visited
          val inRange: Explore.BatchK[O] = _outs.toVector.flatMap { out =>
            val result = openExploring.data.copy(data = out.data)

            val depth = result.depthOpt.getOrElse(Int.MaxValue)

            val reprOpt = if (depth < minRange) {
              Some(
                result.copy(isOutOfRange = true)
              )
            } else if (depth < maxRange) {
              Some(result)
            } else None

            reprOpt.map { e =>
              openExploring.copy(data = e)
            }
          }

          Commit(selectedRow.localityGroup).intoVisited(inRange)
        }

        {
          // recursively generate new openSet
          val fetched: Seq[(Trace, Open.Payload)] = _induction.flatMap {
            case (nexTtraceSet, nextData) =>
              val nextElem: Open.Exploring = openExploring.data.depth_++.copy(nextData.data)

              val nextPayload: Open.Payload = openExploring.copy(data = nextElem)
              nexTtraceSet.asTraceSet.map { trace =>
                trace -> nextPayload
              }
          }

          val grouped: MapView[LocalityGroup, Seq[Open.Payload]] = fetched
            .groupBy(v => LocalityGroup(v._1)().sameBy(sameBy))
            .view
            .mapValues(_.map(_._2))

          // this will be used to filter dataRows yield by the next fork, it will not affect current transformation
          val filtered: List[(LocalityGroup, Seq[Open.Payload])] = grouped.filter {
            case (_, v) =>
              v.nonEmpty
          }.toList

          filtered.foreach { (newOpen: (LocalityGroup, Seq[Open.Payload])) =>
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
