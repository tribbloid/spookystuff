package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.caching.ExploreRunnerCache
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, FetchOptimizer, JoinType}
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row.{SquashedFetchedRow, _}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType}

import scala.util.Random

case class ExploreParams(
                          depthField: Field, //can be null
                          ordinalField: Field, //can be null
                          range: Range,
                          extracts: Seq[Extractor[Any]]
                        ) {

}

//TODO: test if lazy execution works on it.
//TODO: what's the relationship between graph explore and drone explore in Prometheus?
case class ExplorePlan(
                        override val child: ExecutionPlan,

                        on: Alias[FetchedRow, Any],
                        sampler: Sampler[Any],
                        joinType: JoinType,

                        traces: Set[Trace],
                        partitionerFactory: RDD[_] => Partitioner,
                        fetchOptimizer: FetchOptimizer,

                        params: ExploreParams,
                        exploreAlgorithm: ExploreAlgorithm,
                        miniBatch: Int,
                        //TODO: stopping condition can be more than this,
                        //TODO: test if proceed to next batch works
                        checkpointInterval: Int // set to Int.MaxValue to disable checkpointing,
                      ) extends UnaryPlan(child) with InjectBeaconRDDPlan {

  val resolver = child.schema.newResolver

  val _on: Resolved[Any] = resolver.include(on).head
  val _params: ExploreParams = {

    val effectiveDepthField = {
      Option(params.depthField) match {
        case Some(field) =>
          field
        case None =>
          Field(_on.field.name + "_depth", isWeak = true)
      }
    }
      .copy(depthRangeOpt = Some(params.range))

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

  val depth_0: Resolved[Int] = resolver.include(Literal(0) withAlias _params.depthField).head
  val depth_++ : Resolved[Int] = resolver.include(
    GetExpr(_params.depthField).typed[Int].andThen(_ + 1) withAlias _params.depthField.!!
  ).head
  val _ordinal: TypedField = resolver.includeTyped(TypedField(_params.ordinalField, ArrayType(IntegerType))).head
  val _extracts: Seq[Resolved[Any]] = resolver.include(_params.extracts: _*)

  override val schema: DataRowSchema = resolver.build

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

  val impl = exploreAlgorithm.getImpl(_params, this.schema)

  override def doExecute(): SquashedFetchedRDD = {
    assert(_params.depthField != null)

    val execID = Random.nextLong()

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val rowFn: SquashedFetchedRow => SquashedFetchedRow = {
      row: SquashedFetchedRow =>
        row.extract(_extracts: _*)
    }

    val state0RDD: RDD[(Trace, Open_Visited)] = child.rdd()
      .flatMap {
        row0 =>
          val row0WithDepth = row0.extract(depth_0)
          val depth0 = rowFn.apply(row0WithDepth)
          val visited0 = if (_params.range.contains(0)) {
            //extract on selfRDD, add into visited set.
            Some(depth0.traceView.children -> Open_Visited(visited = Some(depth0.dataRows)))
          }
          else {
            None
          }

          val open0 = depth0
            .extract(_on)
            .flattenData(_on.field, _params.ordinalField, joinType.isLeft, sampler)
            .interpolate(traces)
            .map {
              t =>
                t._1 -> Open_Visited(open = Some(Array(t._2)))
            }

          open0 ++ visited0
      }


    val combinedReducer: (Open_Visited, Open_Visited) => Open_Visited = {
      (v1, v2) =>
        Open_Visited (
          open = (v1.open ++ v2.open).map(_.toIterable).reduceOption(impl.openReducerBetweenBatches).map(_.toArray),
          visited = (v1.visited ++ v2.visited).map(_.toIterable).reduceOption(impl.visitedReducerBetweenBatches).map(_.toArray)
        )
    }

    val partitioner0 = partitionerFactory(state0RDD)
    //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()
    val reducedState0RDD: RDD[(Trace, Open_Visited)] = betweenEpochReduce(state0RDD, combinedReducer, partitioner0)

    val openSetSize = spooky.sparkContext.accumulator(0)
    var i = 1
    var stop: Boolean = false

    var stateRDD: RDD[(Trace, Open_Visited)] = reducedState0RDD

    while (!stop) {

      openSetSize.setValue(0)

      val stateRDD_+ : RDD[(Trace, Open_Visited)] = stateRDD.mapPartitions {
        itr =>
          val state = new ExploreRunner(itr, execID)
          val state_+ = state.execute(
            _on,
            sampler,
            joinType,

            traces
          )(
            miniBatch,
            impl,
            depth_++,
            spooky
          )(
            rowFn
          )
          openSetSize += state.open.size
          state_+
      }

      val partitioner_+ = partitionerFactory(stateRDD_+)
      //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()

      val reducedStateRDD_+ : RDD[(Trace, Open_Visited)] = betweenEpochReduce(stateRDD_+, combinedReducer, partitioner_+)

      cacheQueue.persist(reducedStateRDD_+, spooky.conf.defaultStorageLevel)
      if (checkpointInterval >0 && i % checkpointInterval == 0) {
        reducedStateRDD_+.checkpoint()
      }

      reducedStateRDD_+.count()
      if (openSetSize.value == 0) stop = true

      cacheQueue.unpersist(stateRDD)
      stateRDD = reducedStateRDD_+
      i += 1
    }

    val result = stateRDD
      .mapPartitions{
        itr =>
          ExploreRunnerCache.finishJob(execID) //manual cleanup, one per node is enough, one per executor is not too much slower
          itr
      }
      .flatMap {
        v =>
          val visitedOpt = v._2.visited

          visitedOpt.map {
            visited =>
              SquashedFetchedRow(visited, TraceView(children = v._1))
          }
      }

    result
  }

  def betweenEpochReduce(
                      state0RDD: RDD[(Trace, Open_Visited)],
                      reducer: (Open_Visited, Open_Visited) => Open_Visited,
                      partitioner0: Partitioner
                    ): RDD[(Trace, Open_Visited)] = {
    val gp = fetchOptimizer.getGenPartitioner(partitioner0)
    gp.reduceByKey(state0RDD, reducer, beaconRDDOpt)
  }
}