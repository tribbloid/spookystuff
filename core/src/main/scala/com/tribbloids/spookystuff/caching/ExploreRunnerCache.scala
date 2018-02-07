package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.execution.{ExploreRunner, NodeKey}
import com.tribbloids.spookystuff.row.{DataRow, RowReducer}
import com.tribbloids.spookystuff.utils.CachingUtils.{ConcurrentCache, ConcurrentMap, ConcurrentSet}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine
  * This is a makeshift implementation, faster implementation will be based on Google Guava library
  */
object ExploreRunnerCache {

  // (NodeKey, ExecutionID) -> Squashed Rows
  // Long is the exeID that segments Squashed Rows from different jobs
  //TODO: change to UUID
  val committedVisited: ConcurrentCache[(NodeKey, Long), Iterable[DataRow]] = ConcurrentCache()

  val onGoings: ConcurrentMap[Long, ConcurrentSet[ExploreRunner]] = ConcurrentMap() //executionID -> running ExploreStateView

  def getOnGoingRunners(exeID: Long): ConcurrentSet[ExploreRunner] = {
    //    onGoings.synchronized{
    onGoings
      .getOrElseUpdate(
        exeID, {
          val v = ConcurrentSet[ExploreRunner]()
          v
        }
      )
    //    }
  }

  def finishExploreExecutions(exeID: Long): Unit = {
    onGoings -= exeID
  }

  // TODO relax synchronized check to accelerate?
  private def commit1(
                       key: (NodeKey, Long),
                       value: Iterable[DataRow],
                       reducer: RowReducer
                     ): Unit = {

    committedVisited.synchronized{
      val oldVs = committedVisited.get(key)
      val newVs = (Seq(value) ++ oldVs).reduce(reducer)
      committedVisited.put(key, newVs)
    }
  }

  def commit(
              kvs: Iterable[((NodeKey, Long), Iterable[DataRow])],
              reducer: RowReducer
            ): Unit = {

    kvs.foreach{
      kv =>
        commit1(kv._1, kv._2, reducer)
    }
  }

  def register(v: ExploreRunner, exeID: Long): Unit = {
    getOnGoingRunners(exeID) += v
  }

  def deregister(v: ExploreRunner, exeID: Long): Unit = {
    getOnGoingRunners(exeID) -= v
  }

  def get(key: (NodeKey, Long)): Set[Iterable[DataRow]] = {
    val onGoing = this.getOnGoingRunners(key._2)
      .toSet[ExploreRunner]

    val onGoingVisitedSet = onGoing
      .flatMap {
        v =>
          v.visited.get(key._1)
      }

    onGoingVisitedSet ++ committedVisited.get(key)
  }

  def getAll(exeID: Long): Map[NodeKey, Iterable[DataRow]] = {
    val onGoing: Map[NodeKey, Iterable[DataRow]] = this.getOnGoingRunners(exeID)
      .map(_.visited.toMap)
      .reduceOption {
        (v1, v2) =>
          v1 ++ v2
      }
      .getOrElse(Map.empty)

    val commited: Map[NodeKey, Iterable[DataRow]] = committedVisited
      .toMap
      .filterKeys(_._2 == exeID)
      .map {
        case (k, v) =>
          k._1 -> v
      }

    onGoing ++ commited
  }
}