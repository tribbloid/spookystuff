package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.execution.ExploreRunner
import com.tribbloids.spookystuff.row.{DataRow, RowReducer}

/**
  * Singleton, always in the JVM and shared by all executors on the same machine
  * This is a makeshift implementation, faster implementation will be based on Google Guava library
  */
object ExploreRunnerCache {

  //TODO: change to TraceView
  val committedRows: ConcurrentCache[(Trace, Long), Iterable[DataRow]] = ConcurrentCache()

  private val _onGoings: ConcurrentMap[Long, ConcurrentSet[ExploreRunner]] = ConcurrentMap() //jobID -> running ExploreStateView
  def onGoings = _onGoings

  def onGoing(jobID: Long): ConcurrentSet[ExploreRunner] = {
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
                       key: (Trace, Long),
                       value: Iterable[DataRow],
                       reducer: RowReducer
                     ): Unit = {

    val oldVs = committedRows.get(key)
    val newVs = (Seq(value) ++ oldVs).reduce(reducer)
    committedRows.put(key, newVs)
  }

  def commit(
              kvs: Iterable[((Trace, Long), Iterable[DataRow])],
              reducer: RowReducer
            ): Unit = {

    committedRows.synchronized{
      kvs.foreach{
        kv =>
          commit1(kv._1, kv._2, reducer)
      }
    }
  }

  def register(v: ExploreRunner): Unit = {
    onGoing(v.executionID) += v
  }

  def deregister(v: ExploreRunner): Unit = {
    onGoing(v.executionID) -= v
  }

  //  def replaceInto(
  //                   key: (Trace, Long),
  //                   values: Array[DataRow]
  //                 ): this.type = {
  //    this.synchronized{
  //      this.put(key, values)
  //    }
  //
  //    this
  //  }

  def get(
           key: (Trace, Long),
           reducer: RowReducer
         ): Option[Array[DataRow]] = {

    getAll(key)
      .reduceOption(reducer).map(_.toArray)
  }

  def getAll(key: (Trace, Long)): Set[Iterable[DataRow]] = {
    val onGoing = this.onGoing(key._2)
      .toSet[ExploreRunner]

    val onGoingVs = onGoing
      .flatMap {
        v =>
          v.visited.get(key._1)
      }

    onGoingVs ++ committedRows.get(key)
  }
}