package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{HasTraceSet, Trace}
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.{GenPartitioner, PathPlanning, Sampler}
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import java.util.UUID

object ExplorePlan {

  /**
    * [[ChainPlan.Batch]] deliberately contains [[Data.Scoped]], but the scope will not be commited into the visited
    * set. it is only there to make appending [[ChainPlan]] easier
    *
    * @tparam I
    *   input
    * @tparam O
    *   output
    */
  type Batches[I, O] = (FetchPlan.Batch[I], ChainPlan.Batch[O])

  type Fn[I, O] = FetchedRow[Data.Exploring[I]] => Batches[I, O]

  object Invar {

    val proto: FetchPlan.Invar.type = FetchPlan.Invar

    type ResultMag[I] = proto.ResultMag[I]
    type _Fn[I] = FetchedRow[Data.Exploring[I]] => ResultMag[I]

    type Target[I] = Fn[I, Data.Exploring[I]]

//    def normalise[I](
//        fn: _Fn[I],
//        sampler: Sampler = Sampler.Identity
//    ): Fn[I, I] = {
//      val fetchFn: FetchPlan.Fn[Any, Any] = FetchPlan.Invar.normalise(fn)
//
//      { row =>
//        val mag = fn(row)
//
//        val normalised: (HasTraceSet, Data.WithScope[Data.Exploring[I]]) = mag.revoke match {
//          case Left(traces) =>
//            traces -> row.payload
//          case Right(v) =>
//            v._1 -> row.payload.copy(
//              data = row.payload.copy(
//                v._2
//              )
//            )
//        }
//
//        val flat: Seq[(Trace, Data.WithScope[Data.Exploring[I]])] = normalised._1.asTraceSet.map { trace =>
//          trace -> normalised._2
//        }.toSeq
//
//        val sampled = sampler.apply[Yield[I]](flat).map { (opt: Option[Yield[I]]) =>
//          opt.getOrElse {
//            val default: Yield[I] = Trace.NoOp.trace -> normalised._2
//            default
//          }
//        }
//
//        sampled
//      }
//    }
  }

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
      open: Option[Explore.BatchK[I]] = None, // a.k.a. pending row
      visited: Option[Explore.BatchK[O]] = None
  )
}

// TODO: impl is too complex, can it be rewritten as a simple loop of FetchPlan, with shuffling enabled intermittenly?
case class ExplorePlan[I, O](
    override val child: ExecutionPlan[I],
    fn: ExplorePlan.Fn[I, O],
    sameBy: Trace => Any = identity,
    genPartitioner: GenPartitioner,
    params: Params,
    pathPlanning: PathPlanning,
    epochInterval: Int,
    // TODO: enable more flexible stopping condition
    // TODO: test if proceeding to next epoch is necessary
    checkpointInterval: Int // set to Int.MaxValue to disable checkpointing,
) extends UnaryPlan[I, O](child)
    with CanInjectBeaconRDD[O]
    with CanChain[O]
    with Explore.Common[I, O] {

  object Init extends Serializable {

    val _effectiveParams: Params = {

      params // TODO:: remove
    }
  }

  import ExplorePlan.*
  import Init.*

  val pathPlanningImpl: PathPlanning.Impl[I, O] =
    pathPlanning._Impl[I, O](_effectiveParams, this.outputSchema)

  override def execute: SquashedRDD[O] = {

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.dirConf.checkpoint)

    val state0RDD: RDD[(LocalityGroup, State[I, O])] = child.squashedRDD
      .map { row0 =>
        val _row0s: SquashedRow[I] = row0

        _row0s.localityGroup -> State[I, O](
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
              epochInterval
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

          val inRange = inRangeExploring.map(v => v.copy(data = v.data.data))

          val result = SquashedRow[O](v._1, inRange)

          result
        }

      }

    result

  }

  def reduceBetweenEpochs(
      stateRDD: RDD[(LocalityGroup, State[I, O])]
  ): RDD[(LocalityGroup, State[I, O])] = {

    val globalReducer: (State[I, O], State[I, O]) => State[I, O] = { (v1, v2) =>
      val open: Option[Open.Batch] = (v1.open ++ v2.open).toSeq
        .reduceOption(pathPlanningImpl.openReducer_global)
        .map(_.toVector)

      val visited: Option[Visited.Batch] = (v1.visited ++ v2.visited).toSeq
        .reduceOption(pathPlanningImpl.visitedReducer_global)
        .map(_.toVector)

      State(None, open, visited)
    }

    gpImpl.reduceByKey(stateRDD, globalReducer, beaconRDDOpt)
  }

  override def chain[O2](fn: ChainPlan.Fn[O, O2]): ExplorePlan[I, O2] = {

    val newFn: Fn[I, O2] = { row =>
      val out1: (FetchPlan.Batch[I], ChainPlan.Batch[O]) = this.fn(row)

      val out2: ChainPlan.Batch[O2] = out1._2.flatMap { data =>
        val row2 = FetchedRow(row.agentState, data)

        val result = fn(row2)

        result
      }

      out1._1 -> out2
    }

    this.copy(
      fn = newFn
    )
  }
}
