package com.tribbloids.spookystuff.execution

import java.util.UUID

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, GenPartitioner, JoinType}
import com.tribbloids.spookystuff.execution.ExplorePlan.{Open_Visited, Params}
import com.tribbloids.spookystuff.execution.MapPlan.RowMapperFactory
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.{Get, Lit}
import com.tribbloids.spookystuff.row.{SquashedFetchedRow, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType}

import scala.collection.mutable.ArrayBuffer

object ExplorePlan {

  type ExeID = UUID

  def nextExeID(): ExeID = UUID.randomUUID()

  case class Params(
      depthField: Field, //can be null
      ordinalField: Field, //can be null
      range: Range,
      executionID: ExeID = nextExeID()
  ) {}

  // use Array to minimize serialization footage
  case class Open_Visited(
      open: Option[Array[DataRow]] = None,
      visited: Option[Array[DataRow]] = None
  )
}

case class ExplorePlan(
    override val child: ExecutionPlan,
    on: Alias[FetchedRow, Any],
    sampler: Sampler[Any],
    joinType: JoinType,
    traces: Set[Trace],
    keyBy: Trace => Any,
    genPartitioner: GenPartitioner,
    params: Params,
    exploreAlgorithm: ExploreAlgorithm,
    epochSize: Int,
    //TODO: stopping condition can be more than this,
    //TODO: test if proceed to next epoch works
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
      Get(_effectiveParams.depthField).typed[Int].andFn(_ + 1) withAlias _effectiveParams.depthField.!!
    )
    .head
  val _ordinal: TypedField =
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

  val invariantRowMapperFactories: ArrayBuffer[RowMapperFactory] = ArrayBuffer.empty[RowMapperFactory]
  def invariantRowMappers: ArrayBuffer[MapPlan.RowMapper] = {
    invariantRowMapperFactories.map { factory =>
      val rowMapper = factory(_protoSchema)
      rowMapper
    }
  }
  def allRowMappers: List[MapPlan.RowMapper] = _rowMappers ++ invariantRowMappers

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

  val impl = exploreAlgorithm.getImpl(_effectiveParams, this.schema)

  override def doExecute(): SquashedFetchedRDD = {
    assert(_effectiveParams.depthField != null)

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.dirConf.checkpoint)

    val rowMapper: SquashedFetchedRow => SquashedFetchedRow = { row =>
      allRowMappers.foldLeft(row) { (row, rowMapper) =>
        rowMapper(row)
      }
    }

    val state0RDD: RDD[(TraceView, Open_Visited)] = child
      .rdd()
      .flatMap { row0 =>
        val row0WithDepth = row0.extract(depth_0)
        val depth0 = rowMapper.apply(row0WithDepth)
        val visited0 = if (_effectiveParams.range.contains(0)) {
          //extract on selfRDD, add into visited set.
          Some(depth0.traceView -> Open_Visited(visited = Some(depth0.dataRows)))
        } else {
          None
        }

        val open0 = depth0
          .extract(_on)
          .flattenData(_on.field, _effectiveParams.ordinalField, joinType.isLeft, sampler)
          .interpolateAndRewriteLocally(traces)
          .map { t =>
            t._1 -> Open_Visited(open = Some(Array(t._2)))
          }

        (open0 ++ visited0).map {
          case (k, v) =>
            k.keyBy(keyBy) -> v
        }
      }

    val combinedReducer: (Open_Visited, Open_Visited) => Open_Visited = { (v1, v2) =>
      Open_Visited(
        open = (v1.open ++ v2.open).map(_.toIterable).reduceOption(impl.openReducerBetweenEpochs).map(_.toArray),
        visited =
          (v1.visited ++ v2.visited).map(_.toIterable).reduceOption(impl.visitedReducerBetweenEpochs).map(_.toArray)
      )
    }

    //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()
    //    val reducedState0RDD: RDD[(TraceView, Open_Visited)] = betweenEpochReduce(state0RDD, combinedReducer)

    val openSetSize = spooky.sparkContext.longAccumulator
    var i = 1
    var stop: Boolean = false

    var stateRDD: RDD[(TraceView, Open_Visited)] = state0RDD

    while (!stop) {

      openSetSize.reset

      val reduceStateRDD: RDD[(TraceView, Open_Visited)] = betweenEpochReduce(stateRDD, combinedReducer)

      val stateRDD_+ : RDD[(TraceView, Open_Visited)] = reduceStateRDD.mapPartitions { itr =>
        val state = new ExploreRunner(itr, impl, keyBy)
        val state_+ = state.run(
          _on,
          sampler,
          joinType,
          traces
        )(
          epochSize,
          depth_++,
          spooky
        )(
          rowMapper
        )
        openSetSize add state.open.size.toLong
        state_+
      }

      //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()

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

    val result = stateRDD
      .mapPartitions { itr =>
        ExploreRunnerCache.finishExploreExecutions(params.executionID) //manual cleanup, one per node is enough, one per executor is not too much slower
        itr
      }
      .flatMap { v =>
        val visitedOpt = v._2.visited

        visitedOpt.map { visited =>
          SquashedFetchedRow(visited, v._1)
        }
      }

    result
  }

  def betweenEpochReduce(
      stateRDD: RDD[(TraceView, Open_Visited)],
      reducer: (Open_Visited, Open_Visited) => Open_Visited
  ): RDD[(TraceView, Open_Visited)] = {
    gpImpl.reduceByKey[Open_Visited](stateRDD, reducer, beaconRDDOpt)
  }
}
