package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.execution.ExploreRunner
import com.tribbloids.spookystuff.row.{DataRow, RowReducer}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine
  * This is a makeshift implementation, faster implementation will be based on Google Guava library
  */
object ExploreRunnerCache {

  // Long is the jobID that segments DataRows from different jobs
  val committedVisited: ConcurrentCache[(TraceView, Long), Iterable[DataRow]] = ConcurrentCache()

  private val _onGoings: ConcurrentMap[Long, ConcurrentSet[ExploreRunner]] = ConcurrentMap() //jobID -> running ExploreStateView
  def onGoings = _onGoings

  def getOnGoingRunners(jobID: Long): ConcurrentSet[ExploreRunner] = {
    onGoings.synchronized{
      onGoings
        .getOrElse(
          jobID, {
            val v = ConcurrentSet[ExploreRunner]()
            onGoings.put(jobID, v)
            v
          }
        )
    }
  }

  def finishJob(jobID: Long): Unit = {
    onGoings.synchronized{
      onGoings -= jobID
    }
  }

  //TODO: relax synchronized check to accelerate
  private def commit1(
                       key: (TraceView, Long),
                       value: Iterable[DataRow],
                       reducer: RowReducer
                     ): Unit = {

    val oldVs = committedVisited.get(key)
    val newVs = (Seq(value) ++ oldVs).reduce(reducer)
    committedVisited.put(key, newVs)
  }

  def commit(
              kvs: Iterable[((TraceView, Long), Iterable[DataRow])],
              reducer: RowReducer
            ): Unit = {

    committedVisited.synchronized{
      kvs.foreach{
        kv =>
          commit1(kv._1, kv._2, reducer)
      }
    }
  }

  def register(v: ExploreRunner): Unit = {
    getOnGoingRunners(v.executionID) += v
  }

  def deregister(v: ExploreRunner): Unit = {
    getOnGoingRunners(v.executionID) -= v
  }

  //  def replaceInto(
  //                   key: (TraceView, Long),
  //                   values: Array[DataRow]
  //                 ): this.type = {
  //    this.synchronized{
  //      this.put(key, values)
  //    }
  //
  //    this
  //  }

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