package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.function.PreDef
import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.caching.ExploreLocalCache
import com.tribbloids.spookystuff.dsl.{ForkType, GenPartitioner, PathPlanning}
import com.tribbloids.spookystuff.execution.Delta.ToDelta
import com.tribbloids.spookystuff.execution.ExplorePlan.{Params, State}
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.{Get, Lit}
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.slf4j.LoggerFactory

import java.util.UUID

object ExplorePlan {

  type ExeID = UUID

  def nextExeID(): ExeID = UUID.randomUUID()

  case class Params(
      depthField: Field, // can be null
      ordinalField: Field, // can be null
      range: Range,
      executionID: ExeID = nextExeID()
  ) {

    lazy val effectiveRange: Range = {
      require(range.min >= -1, "explore range cannot be lower than -1")
      range
    }

    lazy val includeStateBeforeExplore: Boolean = effectiveRange.contains(-1)
  }

  // use Array to minimize serialization footage
  case class State(
      row0: Option[SquashedRow] = None, // always be executed first
      open: Option[Vector[DataRow]] = None, // a.k.a. pending row
      visited: Option[Vector[DataRow]] = None
  )
}

case class ExplorePlan(
    override val child: ExecutionPlan,
    on: Alias[FetchedRow, Any],
    sampler: Sampler[Any],
    forkType: ForkType,
    traces: TraceSet,
    sameBy: Trace => Any,
    genPartitioner: GenPartitioner,
    params: Params,
    pathPlanning: PathPlanning,
    miniBatchSize: Int,
    // TODO: enable more flexible stopping condition
    // TODO: test if proceeding to next miniBatch is necessary
    checkpointInterval: Int, // set to Int.MaxValue to disable checkpointing,

    toDeltas: List[ToDelta]
) extends UnaryPlan(child)
    with InjectBeaconRDDPlan {

  object Init extends Serializable {

    val resolver: child.outputSchema.Resolver = child.outputSchema.newResolver

    val _on: Resolved[Any] = resolver.include(on).head
    val _effectiveParams: Params = {

      val effectiveDepthField = {
        Option(params.depthField) match {
          case Some(field) =>
            field
          case None =>
            Field(_on.field.name + "_depth").*
        }
      }.copy(depthRangeOpt = Some(params.effectiveRange))

      val effectiveOrdinalField = Option(params.ordinalField) match {
        case Some(ff) =>
          ff.copy(isOrdinal = true)
        case None =>
          Field(_on.field.name + "_ordinal", isOrdinal = true).*
      }

      params.copy(
        ordinalField = effectiveOrdinalField,
        depthField = effectiveDepthField
      )
    }

    val depth_0: Resolved[Int] = resolver.include(Lit(0) withAlias _effectiveParams.depthField).head
    val depth_++ : Resolved[Int] = resolver
      .include(
        Get(_effectiveParams.depthField).typed[Int].andMap(_ + 1) withAlias _effectiveParams.depthField.!!
      )
      .head

    resolver.includeTyped(TypedField(_effectiveParams.ordinalField, ArrayType(IntegerType))).head

    lazy val composedDelta: Delta = {

      val applied = {
        var state = resolver.build -> PreDef.FnImpl.identity[SquashedRow]

        toDeltas.foreach { toDelta =>
          val delta = toDelta(state._1)
          val fn = state._2.andThen(delta.fn)

          state = delta.outputSchema -> fn
        }

        state
      }

      new Delta {
        override def fn = applied._2

        override def outputSchema: SpookySchema = applied._1
      }
    }
  }

  import Init._

  override def computeSchema: SpookySchema = {
    composedDelta.outputSchema
  }

  val impl: PathPlanning.Impl = pathPlanning._Impl(_effectiveParams, this.outputSchema)

  override def execute: SquashedRDD = {
    assert(_effectiveParams.depthField != null)

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.dirConf.checkpoint)

    val state0RDD: RDD[(LocalityGroup, State)] = child.squashedRDD
      .map { row0 =>
        val _row0s = row0.withLineageIDs

        val visited0: Option[Vector[DataRow]] = {
          if (_effectiveParams.includeStateBeforeExplore) {
            // an extra visited row that store the state before explore
            Some(
              _row0s.dataRows.map { v =>
                v.self
              }.toVector
            )
          } else {
            None
          }
        }

        _row0s.agentState.group -> State(
          row0 = Some(_row0s),
          visited = visited0
        )
      }

    val openSetAcc = spooky.sparkContext.longAccumulator
    var miniBatchI = 1
    var stop: Boolean = false

    val finalStateRDD: RDD[(LocalityGroup, State)] = {

      var stateRDD: RDD[(LocalityGroup, State)] = state0RDD

      do {
        openSetAcc.reset

        val stateRDD_+ : RDD[(LocalityGroup, State)] = stateRDD.mapPartitions { itr =>
          val runner = ExploreRunner(itr, impl, sameBy, depth_0, depth_++)
          val state_+ = runner.run(
            _on,
            sampler,
            forkType,
            traces
          )(
            miniBatchSize,
            composedDelta
          )
          openSetAcc add runner.open.size.toLong
          state_+
        }

        scratchRDDPersist(stateRDD_+)
        if (checkpointInterval > 0 && miniBatchI % checkpointInterval == 0) {
          stateRDD_+.checkpoint()
        }

        val nRows = stateRDD_+.count()

        val openSetSize = openSetAcc.value

        LoggerFactory
          .getLogger(this.getClass)
          .info(
            s"MiniBatch $miniBatchI: $nRows total, $openSetSize pending"
          )

        scratchRDDs.unpersist(stateRDD)
        if (openSetSize == 0) {
          stop = true

          stateRDD = stateRDD_+
        } else {
          val next: RDD[(LocalityGroup, State)] = reduceBetweenMiniBatch(stateRDD_+)

          stateRDD = next

          miniBatchI += 1
        }

      } while (!stop)

      stateRDD
    }

    val result: RDD[SquashedRow] = finalStateRDD
      .mapPartitions { itr =>
        ExploreLocalCache.deregisterAll(params.executionID)
        // manual cleanup, one per node is enough, one per executor is not too much slower
        itr
      }
      .flatMap { v =>
        val visitedOpt = v._2.visited

        visitedOpt.map { visited =>
          SquashedRow(AgentState(v._1), visited.map(_.withEmptyScope))
            .withCtx(spooky)
            .resetScope
        }
      }

    result
  }

  def reduceBetweenMiniBatch(
      stateRDD: RDD[(LocalityGroup, State)]
  ): RDD[(LocalityGroup, State)] = {

    val globalReducer: (State, State) => State = { (v1, v2) =>
      val open: Option[Vector[DataRow]] = (v1.open ++ v2.open).toSeq
        .reduceOption(impl.openReducer_global)
        .map(_.toVector)

      val visited: Option[Vector[DataRow]] = (v1.visited ++ v2.visited).toSeq
        .reduceOption(impl.visitedReducer_global)
        .map(_.toVector)

      State(None, open, visited)
    }

    gpImpl.reduceByKey(stateRDD, globalReducer, beaconRDDOpt)
  }

}
