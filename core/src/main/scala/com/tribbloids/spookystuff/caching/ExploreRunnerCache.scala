package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.execution.ExploreRunner
import com.tribbloids.spookystuff.row.{DataRow, RowReducer}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine
  * This is a makeshift implementation, faster implementation will be based on Google Guava library
  */
object ExploreRunnerCache {

  // Long is the exeID that segments DataRows from different jobs
  val committedVisited: ConcurrentCache[(TraceView, Long), Iterable[DataRow]] = ConcurrentCache()

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
                       key: (TraceView, Long),
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
              kvs: Iterable[((TraceView, Long), Iterable[DataRow])],
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

  def get(
           key: (TraceView, Long),
           reducer: RowReducer
         ): Option[Array[DataRow]] = {

    getAll(key)
      .reduceOption(reducer).map(_.toArray)
  }

  def getAll(key: (TraceView, Long)): Set[Iterable[DataRow]] = {
    val onGoing = this.getOnGoingRunners(key._2)
      .toSet[ExploreRunner]

    val onGoingVs = onGoing
      .flatMap {
        v =>
          v.visited.get(key._1)
      }

    onGoingVs ++ committedVisited.get(key)
  }
}