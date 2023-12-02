package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Action, Trace, TraceSet}
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, ForkType, GenPartitioner}
import com.tribbloids.spookystuff.execution.ExplorePlan.{Open_Visited, Params}
import com.tribbloids.spookystuff.execution.MapPlan.RowMapperFactory
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.{Get, Lit}
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType}

import java.util.UUID

object ExplorePlan {

  type ExeID = UUID

  def nextExeID(): ExeID = UUID.randomUUID()

  case class Params(
      depthField: Field, // can be null
      ordinalField: Field, // can be null
      range: Range,
      executionID: ExeID = nextExeID()
  ) {}

  // use Array to minimize serialization footage
  case class Open_Visited(
      open: Option[Vector[DataRow]] = None,
      visited: Option[Vector[DataRow]] = None
  )
}

case class ExplorePlan(
    override val child: ExecutionPlan,
    on: Alias[FetchedRow, Any],
    sampler: Sampler[Any],
    forkType: ForkType,
    traces: TraceSet,
    keyBy: List[Action] => Any,
    genPartitioner: GenPartitioner,
    params: Params,
    exploreAlgorithm: ExploreAlgorithm,
    miniBatchSize: Int,
    // TODO: enable more flexible stopping condition
    // TODO: test if proceeding to next miniBatch is necessary
    checkpointInterval: Int, // set to Int.MaxValue to disable checkpointing,

    //                          extracts: Seq[Extractor[Any]],
    rowMapperFactories: List[RowMapperFactory]
) extends UnaryPlan(child)
    with InjectBeaconRDDPlan {

  val resolver: child.schema.Resolver = child.schema.newResolver

  val _on: Resolved[Any] = resolver.include(on).head
  val _effectiveParams: Params = {

    val effectiveDepthField = {
      Option(params.depthField) match {
        case Some(field) =>
          field
        case None =>
          Field(_on.field.name + "_depth", isWeak = true)
      }
    }.copy(depthRangeOpt = Some(params.range))

    val effectiveOrdinalField = Option(params.ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(_on.field.name + "_ordinal", isWeak = true, isOrdinal = true)
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

  val _protoSchema: SpookySchema = resolver.build

  val (_finalSchema, _rowMappers) = {
    var prevSchema = _protoSchema

    val rowMappers = rowMapperFactories.map { factory =>
      val rowMapper = factory(prevSchema)
      prevSchema = rowMapper.schema
      rowMapper
    }

    prevSchema -> rowMappers
  }

  override val schema: SpookySchema = _finalSchema

  def allRowMappers: List[MapPlan.RowMapper] = _rowMappers

  //  {
  //    val extractFields = _extracts.map(_.field)
  //    val newFields = extractFields ++ Option(params.depthField) ++ Option(params.ordinalField)
  //    newFields.groupBy(identity).foreach{
  //      v =>
  //        if (v._2.size > 1) throw new QueryException(s"Field ${v._1.name} already exist")
  //    }
  //    child.schema ++#
  //      Option(params.depthField) ++#
  //      Option(params.ordinalField) ++
  //      _extracts.map(_.typedField)
  //  }

  val impl: ExploreAlgorithm.Impl = exploreAlgorithm.getImpl(_effectiveParams, this.schema)

  override def doExecute(): BottleneckRDD = {
    assert(_effectiveParams.depthField != null)

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.dirConf.checkpoint)

    val rowMapper: BottleneckRow => BottleneckRow = { row =>
      allRowMappers.foldLeft(row) { (row, rowMapper) =>
        val rm: MapPlan.RowMapper = rowMapper

        rm(row)
      }
    }

    val state0RDD: RDD[(Trace, Open_Visited)] = child.bottleneckRDD
      .flatMap { row0 =>
        val row0WithDepth = row0.extract(depth_0)
        val row0Mapped = rowMapper.apply(row0WithDepth)

        val open0 = row0Mapped
          .extract(_on)
          .explodeData(_on.field, _effectiveParams.ordinalField, forkType, sampler)
          .interpolateAndRewrite(traces)
          .map { t =>
            t._1 -> Open_Visited(open = Some(Vector(t._2)))
          }

        val visited0 = {
          if (_effectiveParams.range.contains(-1)) {
            Some(row0Mapped.traceView -> Open_Visited(visited = Some(row0Mapped.dataRows)))
          } else {
            None
          }
        }

        (open0 ++ visited0).map {
          case (k, v) =>
            k.setSamenessFn(keyBy) -> v
        }
      }

    val openSetSize = spooky.sparkContext.longAccumulator
    var i = 1
    var stop: Boolean = false

    val finalStateRDD: RDD[(Trace, Open_Visited)] = {

      var stateRDD: RDD[(Trace, Open_Visited)] = state0RDD

      while (!stop) {

        openSetSize.reset

        val reduceStateRDD: RDD[(Trace, Open_Visited)] = reduceBetweenMiniBatch(stateRDD, impl.combineGlobally)

        val stateRDD_+ : RDD[(Trace, Open_Visited)] = reduceStateRDD.mapPartitions { itr =>
          val localRunner = new ExploreAlgorithmRunner(itr, impl, keyBy)
          val state_+ = localRunner.run(
            _on,
            sampler,
            forkType,
            traces
          )(
            miniBatchSize,
            depth_++
          )(
            rowMapper
          )
          openSetSize add localRunner.open.size.toLong
          state_+
        }

        // this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()

        //      val reducedStateRDD_+ : RDD[(TraceView, Open_Visited)] = betweenEpochReduce(stateRDD_+, combinedReducer)

        persist(stateRDD_+, spooky.spookyConf.defaultStorageLevel)
        if (checkpointInterval > 0 && i % checkpointInterval == 0) {
          stateRDD_+.checkpoint()
        }

        stateRDD_+.count()
        scratchRDDs.unpersist(stateRDD)
        if (openSetSize.value == 0) stop = true

        stateRDD = stateRDD_+
        i += 1
      }

      stateRDD
    }

    val result: RDD[BottleneckRow] = finalStateRDD
      .mapPartitions { itr =>
        ExploreRunnerCache.finishExploreExecutions(
          params.executionID
        ) // manual cleanup, one per node is enough, one per executor is not too much slower
        itr
      }
      .flatMap { v =>
        val visitedOpt = v._2.visited

        visitedOpt.map { visited =>
          BottleneckRow(visited, v._1)
        }
      }

    result
  }

  def reduceBetweenMiniBatch(
      stateRDD: RDD[(Trace, Open_Visited)],
      reducer: (Open_Visited, Open_Visited) => Open_Visited
  ): RDD[(Trace, Open_Visited)] = {
    gpImpl.reduceByKey[Open_Visited](stateRDD, reducer, beaconRDDOpt)
  }
}
