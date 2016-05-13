package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.caching.ExploreSharedVisitedCache
import com.tribbloids.spookystuff.dsl.{FetchOptimizer, FetchOptimizers, JoinType}
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row.{SquashedPageRow, _}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.util.Random

//TODO: test if lazy execution works on it.
case class ExplorePlan(
                        child: ExecutionPlan,

                        expr: Expression[Any],
                        sampler: Sampler[Any],
                        joinType: JoinType,

                        traces: Set[Trace],
                        partitionerFactory: RDD[_] => Partitioner,
                        fetchOptimizer: FetchOptimizer,

                        algorithmImpl: ExploreAlgorithmImpl,
                        miniBatch: Int,
                        //TODO: stopping condition can be more than this,
                        //TODO: test if proceed to next batch works
                        checkpointInterval: Int // set to Int.MaxValue to disable checkpointing,

                      ) extends ExecutionPlan(
  child,
  {
    val extractFields = algorithmImpl.extracts.map(_.field)
    val newFields = extractFields ++ Option(algorithmImpl.depthField) ++ Option(algorithmImpl.ordinalField)
    newFields.groupBy(identity).foreach{
      v =>
        if (v._2.size > 1) throw new QueryException(s"Field ${v._1.name} already exist")
    }
    Some(child.schema ++ Option(algorithmImpl.depthField) ++ Option(algorithmImpl.ordinalField) ++ extractFields)
  }
) with CreateOrInheritBeaconRDDPlan {
  //TODO: detect in-plan field conflict!

  import com.tribbloids.spookystuff.utils.Implicits._

  override def doExecute(): SquashedRowRDD = {
    assert(algorithmImpl.depthField != null)

    val execID = Random.nextLong()

    if (spooky.sparkContext.getCheckpointDir.isEmpty && checkpointInterval > 0)
      spooky.sparkContext.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val rowFn: SquashedPageRow => SquashedPageRow = {
      row: SquashedPageRow =>
        row.extract(algorithmImpl.extracts: _*)
    }

    val state0RDD: RDD[(Trace, Open_Visited)] = child.rdd(true)
      .flatMap {
        row0 =>
          val depth0 = rowFn.apply(row0.extract(Literal(0) ~ algorithmImpl.depthField))
          val visited0 = if (algorithmImpl.range.contains(0)) {
            //extract on selfRDD, add into visited set.
            Some(depth0.trace -> Open_Visited(visited = Some(depth0.dataRows)))
          }
          else {
            None
          }

          val open0 = depth0
            .extract(expr)
            .flattenData(expr.field, algorithmImpl.ordinalField, joinType.isLeft, sampler)
            .interpolate(traces, spooky)
            .map {
              t =>
                t._1 -> Open_Visited(open = Some(Array(t._2)))
            }

          open0 ++ visited0
      }

    val combinedReducer: (Open_Visited, Open_Visited) => Open_Visited = {
      (v1, v2) =>
        Open_Visited (
          open = (v1.open ++ v2.open).map(_.toIterable).reduceOption(algorithmImpl.openReducer).map(_.toArray),
          visited = (v1.visited ++ v2.visited).map(_.toIterable).reduceOption(algorithmImpl.visitedReducer).map(_.toArray)
        )
    }

    val partitioner0 = partitionerFactory(state0RDD)
    //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()
    val reducedState0RDD: RDD[(Trace, Open_Visited)] = preFetchReduce(state0RDD, combinedReducer, partitioner0)

    val openSetSize = spooky.sparkContext.accumulator(0)
    var i = 1
    var stop: Boolean = false

    var stateRDD: RDD[(Trace, Open_Visited)] = reducedState0RDD

    while (!stop) {

      openSetSize.setValue(0)

      val stateRDD_+ : RDD[(Trace, Open_Visited)] = stateRDD.mapPartitions {
        itr =>
          val state = new ExploreLocalExecutor(itr, execID)
          val state_+ = state.execute(
            expr,
            sampler,
            joinType,

            traces
          )(
            miniBatch,
            algorithmImpl,
            spooky
          )(
            rowFn
          )
          openSetSize += state.open.size
          state_+
      }

      val partitioner_+ = partitionerFactory(stateRDD_+)
      //this will use globalReducer, same thing will happen to later states, eliminator however will only be used inside ExploreStateView.execute()

      val reducedStateRDD_+ : RDD[(Trace, Open_Visited)] = preFetchReduce(stateRDD_+, combinedReducer, partitioner_+)

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
          ExploreSharedVisitedCache.finishJob(execID) //manual cleanup, one per node is enough, one per executor is not too much slower
          itr
      }
      .flatMap {
        v =>
          val visitedOpt = v._2.visited

          visitedOpt.map {
            visited =>
              SquashedPageRow(visited, v._1)
          }
      }

    result
  }

  def preFetchReduce(
                       state0RDD: RDD[(Trace, Open_Visited)],
                       reducer: (Open_Visited, Open_Visited) => Open_Visited,
                       partitioner0: Partitioner
                     ): RDD[(List[Action], Open_Visited)] = {
    fetchOptimizer match {
      case FetchOptimizers.Narrow =>
        state0RDD.reduceByKey_narrow(reducer)
      case FetchOptimizers.Wide =>
        state0RDD.reduceByKey(partitioner0, reducer)
      case FetchOptimizers.WebCacheAware =>
        state0RDD.reduceByKey_beacon(reducer, localityBeaconRDDOpt.get)
      case _ => throw new NotImplementedError(s"${fetchOptimizer.getClass.getSimpleName} optimizer is not supported")
    }
  }
}