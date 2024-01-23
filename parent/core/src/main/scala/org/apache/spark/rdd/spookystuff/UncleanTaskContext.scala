package org.apache.spark.rdd.spookystuff

import ai.acyclic.prover.commons.util.Caching
import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

import java.util
import java.util.EventListener

case class UncleanTaskContext(
    self: TaskContext
) extends TaskContext
    with AutoCloseable {

  override def isCompleted(): Boolean = self.isCompleted()

  override def isInterrupted(): Boolean = self.isInterrupted()

  override def cpus(): Int = self.cpus()

  override def resources(): Map[String, ResourceInformation] = self.resources()

  override def resourcesJMap(): util.Map[String, ResourceInformation] = self.resourcesJMap()

  lazy val listeners: Caching.ConcurrentMap[Long, EventListener] =
    Caching.ConcurrentMap[Long, EventListener]()

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    listeners += (System.currentTimeMillis() -> listener)

    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext =
    self.addTaskFailureListener(listener)

  override def stageId(): Int = self.stageId()

  override def stageAttemptNumber(): Int = self.stageAttemptNumber()

  override def partitionId(): Int = self.partitionId()

  override def attemptNumber(): Int = self.attemptNumber()

  override def taskAttemptId(): Long = self.taskAttemptId()

  override def getLocalProperty(key: String): String = self.getLocalProperty(key)

  override def taskMetrics(): TaskMetrics = self.taskMetrics()

  override def getMetricsSources(sourceName: String): Seq[Source] = self.getMetricsSources(sourceName)

  override private[spark] def killTaskIfInterrupted(): Unit = self.killTaskIfInterrupted()

  override private[spark] def getKillReason() = self.getKillReason()

  override private[spark] def taskMemoryManager() = self.taskMemoryManager()

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = self.registerAccumulator(a)

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = self.fetchFailed

  override private[spark] def markInterrupted(reason: String): Unit = self.markInterrupted(reason)

  override private[spark] def markTaskFailed(error: Throwable): Unit = self.markTaskFailed(error)

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = self.markTaskCompleted(error)

  override private[spark] def fetchFailed = self.fetchFailed

  override private[spark] def getLocalProperties = self.getLocalProperties

  override def close(): Unit = {

    listeners.keys.toList.sorted.foreach { time =>
      listeners.get(time).foreach {
        case v: TaskCompletionListener =>
          v.onTaskCompletion(self)
      }
    }
  }

  override def numPartitions(): Int = self.numPartitions()
}
