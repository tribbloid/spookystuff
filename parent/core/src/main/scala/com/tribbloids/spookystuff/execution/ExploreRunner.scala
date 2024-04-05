package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.dsl.PathPlanning
import com.tribbloids.spookystuff.execution.ExplorePlan.{ExeID, State}
import com.tribbloids.spookystuff.row._

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

  import pathPlanningImpl.params._

  def exeID: ExeID = pathPlanningImpl.params.executionID

  lazy val spooky: SpookyContext = pathPlanningImpl.schema.ctx

  // TODO: add fast sorted implementation
  val open: ConcurrentMap[LocalityGroup, Vector[Elem]] =
    ConcurrentMap()

  val visited: ConcurrentMap[LocalityGroup, Outs] = ConcurrentMap()

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
          v.copy(agentState = v.agentState.copy(group = _group))
        }
    }
  }

  def isFullyExplored: Boolean = row0Partition.isEmpty && open.isEmpty

  @volatile var fetchingInProgressOpt: Option[LocalityGroup] = None

  private case class Commit(
      group: LocalityGroup
  ) {

    def intoOpen(
        value: Vector[Elem],
        reducer: OpenReducer = pathPlanningImpl.openReducer
    ): mutable.Map[LocalityGroup, Vector[Elem]] = {
      val oldVs: Vector[Elem] = open.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      open += group -> newVs
    }

    def intoVisited(
        value: Vector[Out],
        reducer: VisitedReducer = pathPlanningImpl.visitedReducer
    ): mutable.Map[LocalityGroup, Vector[Out]] = {
      val oldVs: Vector[Out] = visited.getOrElse(group, Vector.empty)
      val newVs = reducer(value, oldVs)
      visited += group -> newVs
    }
  }

  def commitVisitedIntoLocalCache(
      reducer: VisitedReducer
  ): Unit = {

    // TODO relax synchronized check to accelerate?
    def commit1(
        key: (LocalityGroup, ExeID),
        value: Outs
    ): Unit = {

      val exe = ExploreLocalCache.getExecution[I, O](key._2)

      exe.visited.synchronized {
        val oldVs: Option[Outs] = exe.visited.get(key._1)
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

  private def selectNext(): SquashedRow[Elem] = {

    val selectedRow: SquashedRow[Elem] = {
      val row0Opt: Option[SquashedRow[Elem]] = row0Partition
        .nextOption()
        .map { row =>
          row.exploring
        }

      val row: SquashedRow[Elem] = row0Opt
        .getOrElse {

          val selected: (LocalityGroup, Elems) = pathPlanningImpl.selectNextOpen(open)
          val withLineage: SquashedRow[Elem] =
            SquashedRow(AgentState(selected._1), selected._2.map(v => Data.WithScope.empty(v)))
              .withCtx(spooky)
              .withDefaultScope

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
      fn: Fn
  ) {

    private def outer = ExploreRunner.this

    def once(): Unit = {

      val selectedRow: SquashedRow[Elem] = selectNext()

      if (selectedRow.dataSeq.isEmpty) return

      outer.fetchingInProgressOpt = Some(selectedRow.group)

      val unSquashedRows: Seq[FetchedRow[Elem]] = selectedRow.withCtx(spooky).unSquash

      val next = unSquashedRows.map { in =>
        val elem: Elem = in.data

        val (_forkedTuples, _outs) = fn(in)

        {
          // commit out into visited
          val inRange: Vector[Data.Exploring[O]] = _outs.toVector.flatMap { out =>
            val result = elem.copy(data = out)

            val depth = result.depthOpt.getOrElse(Int.MaxValue)

            if (depth < minRange) {
              Some(
                result.copy(isOutOfRange = true)
              )
            } else if (depth < maxRange) {
              Some(result)
            } else None
          }

          Commit(selectedRow.group).intoVisited(inRange)
        }

        {
          // recursively generate new openSet
          val fetched: Seq[(Trace, Elem)] = _forkedTuples.flatMap {
            case (traceSet, next) =>
              val nextElem: Elem = elem.depth_++.copy(next)
              traceSet.map { trace =>
                trace -> nextElem
              }
          }

          val grouped: MapView[LocalityGroup, Seq[Elem]] = fetched
            .groupBy(v => LocalityGroup(v._1)().sameBy(sameBy))
            .view
            .mapValues(_.map(_._2))

          // this will be used to filter dataRows yield by the next fork, it will not affect current transformation
          val filtered: List[(LocalityGroup, Seq[Elem])] = grouped.filter {
            case (_, v) =>
              v.nonEmpty
          }.toList

          filtered.foreach { newOpen: (LocalityGroup, Seq[Elem]) =>
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
