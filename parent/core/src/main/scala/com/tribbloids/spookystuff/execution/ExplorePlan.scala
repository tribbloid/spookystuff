package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.{GenPartitioner, PathPlanning}
import com.tribbloids.spookystuff.execution.ExplorePlan.{Params, State}
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import java.util.UUID

object ExplorePlan {

  /**
    * [[FlatMapPlan.Batch]] deliberately contains [[Data.WithScope]], but the scope will not be commited into the
    * visited set. it is only there to make appending [[FlatMapPlan]] easier
    *
    * @tparam I
    *   input
    * @tparam O
    *   output
    */
  type Batches[I, O] = (FetchPlan.Batch[I], FlatMapPlan.Batch[O])

  type Fn[I, O] = FetchedRow[Data.Exploring[I]] => Batches[I, O]

  type ExeID = UUID
  def nextExeID(): ExeID = UUID.randomUUID()

  case class Params(
      private val _range: Range,
      executionID: ExeID = nextExeID()
  ) {

    @transient lazy val range: Range = {
      require(_range.min >= 0, "explore range cannot be lower than 0")
      _range
    }

    @transient lazy val maxRange: Int = range.max
    @transient lazy val minRange: Int = range.min
  }

  // use Array to minimize serialization footage
  case class State[I, O](
      row0: Option[SquashedRow[I]] = None, // always be executed first
      open: Option[Vector[Data.Exploring[I]]] = None, // a.k.a. pending row
      visited: Option[Vector[Data.Exploring[O]]] = None
  )
}

case class ExplorePlan[I, O](
    override val child: ExecutionPlan[I],
    fn: ExplorePlan.Fn[I, O],
    sameBy: Trace => Any = identity,
    genPartitioner: GenPartitioner,
    params: Params,
    pathPlanning: PathPlanning,
    balancingInterval: Int,
    // TODO: enable more flexible stopping condition
    // TODO: test if proceeding to next epoch is necessary
    checkpointInterval: Int // set to Int.MaxValue to disable checkpointing,
) extends UnaryPlan[I, O](child)
    with InjectBeaconRDDPlan[O]
    with Explore.Common[I, O] {

  object Init extends Serializable {

    val _effectiveParams: Params = {

      params // TODO:: remove
    }

  }

  import Init._

  val pathPlanningImpl: PathPlanning.Impl[I, O] =
    pathPlanning._Impl[I, O](_effectiveParams, this.outputSchema)

  override def execute: SquashedRDD[O] = {

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.dirConf.checkpoint)

    val state0RDD: RDD[(LocalityGroup, State[I, O])] = child.squashedRDD
      .map { row0 =>
        val _row0s: SquashedRow[I] = row0

        _row0s.agentState.group -> State[I, O](
          row0 = Some(_row0s),
          visited = None
        )
      }

    val openSetAcc = spooky.sparkContext.longAccumulator

    var epochI = 1
    var stop: Boolean = false

    val finalStateRDD: RDD[(LocalityGroup, State[I, O])] = {

      var stateRDD: RDD[(LocalityGroup, State[I, O])] = state0RDD

      do {
        openSetAcc.reset

        val stateRDD_+ : RDD[(LocalityGroup, State[I, O])] = stateRDD.mapPartitions { itr =>
          val runner = ExploreRunner(itr, pathPlanningImpl, sameBy)
          val state_+ = runner
            .Run(fn)
            .recursively(
              balancingInterval
            )
          openSetAcc add runner.open.size.toLong
          state_+
        }

        scratchRDDPersist(stateRDD_+)
        if (checkpointInterval > 0 && epochI % checkpointInterval == 0) {
          stateRDD_+.checkpoint()
        }

        val nRows = stateRDD_+.count()

        val openSetSize = openSetAcc.value

        LoggerFactory
          .getLogger(this.getClass)
          .info(
            s"Epoch $epochI: $nRows total, $openSetSize pending"
          )

        scratchRDDs.unpersist(stateRDD)
        if (openSetSize == 0) {
          stop = true

          stateRDD = stateRDD_+
        } else {
          val next: RDD[(LocalityGroup, State[I, O])] = reduceBetweenEpochs(stateRDD_+)

          stateRDD = next

          epochI += 1
        }

      } while (!stop)

      stateRDD
    }

    val result: RDD[SquashedRow[O]] = finalStateRDD
      .mapPartitions { itr =>
        ExploreLocalCache.deregisterAll(params.executionID)
        // manual cleanup, one per node is enough, one per executor is not too much slower
        itr
      }
      .flatMap { v =>
        val visitedOpt = v._2.visited
        visitedOpt.map { visited =>
          val inRangeExploring = visited.filterNot { row =>
            row.isOutOfRange
          }

          val inRange = inRangeExploring.map(v => Data.WithScope.empty(v.data))

          val result = SquashedRow[O](AgentState(v._1), inRange)
            .withCtx(spooky)
            .withDefaultScope

          result
        }

      }

    result

  }

  def reduceBetweenEpochs(
      stateRDD: RDD[(LocalityGroup, State[I, O])]
  ): RDD[(LocalityGroup, State[I, O])] = {

    val globalReducer: (State[I, O], State[I, O]) => State[I, O] = { (v1, v2) =>
      val open: Option[Elems] = (v1.open ++ v2.open).toSeq
        .reduceOption(pathPlanningImpl.openReducer_global)
        .map(_.toVector)

      val visited: Option[Outs] = (v1.visited ++ v2.visited).toSeq
        .reduceOption(pathPlanningImpl.visitedReducer_global)
        .map(_.toVector)

      State(None, open, visited)
    }

    gpImpl.reduceByKey(stateRDD, globalReducer, beaconRDDOpt)
  }

  //    this match {
  //      case plan: ExplorePlan[_, _] if !this.isCached =>
  //        object _More extends Explore.Fn[D, O] {
  //
  //          override def apply(row: FetchedRow[Data.Exploring[D]]) = {
  //
  //            val (forked, flat) = plan.fn(row)
  //
  //            flat.map(withScope => ???)
  //            ???
  //          }
  //        }
  //
  //        plan.copy()(_More)
  //      case _ =>
  //        FlatPlan(this, fn)
  //    }
//  }
}
