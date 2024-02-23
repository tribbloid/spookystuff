package com.tribbloids.spookystuff.commons.lifespan

import ai.acyclic.prover.commons.same.EqualBy
import org.apache.spark.sql._SQLHelper
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{SparkEnv, TaskContext}

object LifespanContext {}

case class LifespanContext(
    @transient _task: TaskContext = TaskContext.get(),
    @transient thread: Thread = Thread.currentThread()
) extends EqualBy {

  @transient lazy val taskOpt: Option[TaskContext] = Option(_task)

  def task: TaskContext = taskOpt.getOrElse(
    throw new UnsupportedOperationException("Not inside any Spark Task")
  )

  val taskAttemptID: Option[Long] = taskOpt.map(_.taskAttemptId())
  val threadID: Long = thread.getId
  val stageID: Option[Int] = taskOpt.map(_.stageId())

  @transient lazy val _sparkEnvOpt: Option[SparkEnv] = Option(SparkEnv.get)

  val blockManagerID: Option[BlockManagerId] = _sparkEnvOpt.map(_.blockManager.blockManagerId)
  val executorID: Option[String] = _sparkEnvOpt.map(_.executorId)

  override val samenessDelegatedTo: (Option[Long], Long) = taskAttemptID -> threadID

  val threadStr: String = {
    "Thread-" + thread.getId + s"[${thread.getName}]" + {

      var suffix: Seq[String] = Nil
      if (thread.isInterrupted) suffix :+= "interrupted"
      else if (!thread.isAlive) suffix :+= "dead"

      if (suffix.isEmpty) ""
      else suffix.mkString("(", ",", ")")
    }
  }

  val taskStr: String = taskOpt
    .map { task =>
      "task " + task.taskAttemptId() + {
        var suffix: Seq[String] = Nil
        if (task.isCompleted()) suffix :+= "completed"
        if (task.isInterrupted()) suffix :+= "interrupted"

        if (suffix.isEmpty) ""
        else suffix.mkString("(", ",", ")")
      }
    }
    .getOrElse("")

  val taskLocationStr: Option[String] = _SQLHelper.taskLocationStrOpt

  override def toString: String =
    if (taskStr.isEmpty || threadStr.contains(taskStr)) {
      threadStr
    } else {
      threadStr + "[" + taskStr + "]"
    }

  /**
    * @return
    *   true if any of the thread is dead OR the task is completed
    */
  def isCompleted: Boolean = {
    !thread.isAlive ||
    taskOpt.exists(_.isCompleted())
  }
}
