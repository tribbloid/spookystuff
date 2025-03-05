package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.dsl.{Locality, PathPlanning}
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel

object ExplorePlan {

  /**
    * [[FlatMapPlan.Batch]] deliberately contains [[Data.Scoped]], but the scope will not be commited into the visited
    * set. it is only there to make appending [[FlatMapPlan]] easier
    *
    * @tparam I
    *   input
    * @tparam O
    *   output
    */
  type Batch[I, O] = (FetchPlan.Batch[I], FlatMapPlan.Batch[O])

  type Fn[I, O] = FetchedRow[Data.Exploring[I]] => Batch[I, O]

  /**
    * special case in which the output data uses the recursive data directly
    *
    * the only case supported by [[com.tribbloids.spookystuff.rdd.DataView.recursively]]
    *
    * to get other cases, you need to define a [[FlatMapPlan]] and let query optimiser to merge it into [[ExplorePlan]]
    */
  object Invar {

    type _Batch[I] = Batch[I, Data.Exploring[I]]

    type _Fn[I] = Fn[I, Data.Exploring[I]]

    def normalise[I](
        fn: FetchPlan.Fn[Data.Exploring[I], I]
    ): _Fn[I] = { v =>
      val left: FetchPlan.Batch[I] = fn(v)

      val right = left.map(_._2).map { nextRaw =>
        val nextExploring = v.data.copy(
          raw = nextRaw
        )

        nextExploring
      }

      val result: _Batch[I] = left -> right

      result
    }
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
  case class State[I, +O](
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
    genPartitioner: Locality,
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

  @transient lazy val pathPlanningImpl: PathPlanning.Impl[I, O] =
    pathPlanning._Impl[I, O](_effectiveParams, this.outputSchema)

  @transient lazy val state0RDD: RDD[(LocalityGroup, State[I, O])] = {
    child.squashedRDD.count()

//    ctx.blockUntilKilled(1000000)

    child.squashedRDD
      .map { row0 =>
        val _row0s: SquashedRow[I] = row0

        _row0s.exploring.state0
      }
  }

  override def prepare: SquashedRDD[O] = {

    if (ctx.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      ctx.sparkContext.setCheckpointDir(ctx.dirConf.checkpoint)

    val openSetAcc = ctx.sparkContext.longAccumulator

    var epochI = 1
    var stop: Boolean = false

    val finalStateRDD: RDD[(LocalityGroup, State[I, O])] = {

      var stateRDD: RDD[(LocalityGroup, State[I, O])] = state0RDD

      do {
        openSetAcc.reset

        val stateRDD_+ : RDD[(LocalityGroup, State[I, O])] = stateRDD.mapPartitions { itr =>
          val _itr = itr

          val runner = ExploreRunner(_itr, pathPlanningImpl, sameBy)
          val states_+ = runner
            .Run(fn)
            .recursively(
              epochInterval
            )

          openSetAcc add runner.open.size.toLong
          states_+
        }

        if (checkpointInterval > 0 && epochI % checkpointInterval == 0) {
          stateRDD_+.checkpoint()
// no need to persist, checkpoint is stronger
//          print_@("checkpoint")

        } else {
          tempRefs.persist(stateRDD_+, ctx.conf.defaultStorageLevel)

          assert(stateRDD_+.getStorageLevel != StorageLevel.NONE)
//          print_@("persist")
        }

        val nRows = stateRDD_+.count()

        val openSetSize = openSetAcc.value

        LoggerFactory
          .getLogger(this.getClass)
          .info(
            s"Epoch $epochI: $nRows total, $openSetSize pending"
          )

        tempRefs.unpersist(stateRDD)
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
//      .mapPartitions { itr => // TODO: deregister
//        ExploreLocalCache.deregisterAll(params.executionID)
//        // manual cleanup, one per node is enough, one per executor is not too much slower
//        itr
//      }
      .flatMap { v =>
        val visitedOpt = v._2.visited
        visitedOpt.map { visited =>
          val inRangeExploring = visited.filterNot { row =>
            row.isOutOfRange
          }

          val inRange = inRangeExploring.map(v => v.raw)

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

  override def chain[O2: ClassTag](fn: FlatMapPlan.Fn[O, O2]): ExplorePlan[I, O2] = {
    val newFn: Fn[I, O2] = { row =>
      val out1: (FetchPlan.Batch[I], FlatMapPlan.Batch[O]) = this.fn(row)

      val out2: FlatMapPlan.Batch[O2] = out1._2.flatMap { data =>
        val row2 = row.copy(data = data)

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
