package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.storage.BlockManagerId

object LifespanContext {

}

case class LifespanContext(
                            @transient task: TaskContext = TaskContext.get(),
                            @transient thread: Thread = Thread.currentThread()
                          ) extends IDMixin {

  @transient lazy val taskOpt: Option[TaskContext] = Option(task)

  val taskAttemptID: Option[Long] = taskOpt.map(_.taskAttemptId())
  val threadID: Long = thread.getId
  val stageID: Option[Int] = taskOpt.map(_.stageId())

  @transient lazy val _sparkEnv: SparkEnv = SparkEnv.get

  val blockManagerID: BlockManagerId = _sparkEnv.blockManager.blockManagerId
  val executorID: String = _sparkEnv.executorId

  override val _id: (Option[Long], Long) = taskAttemptID -> threadID

  val threadStr: String = {
    "Thread-" + thread.getId + s"[${thread.getName}]" +
      {

        var suffix: Seq[String] = Nil
        if (thread.isInterrupted) suffix :+= "interrupted"
        else if (!thread.isAlive) suffix :+= "dead"

        if (suffix.isEmpty) ""
        else suffix.mkString("(",",",")")
      }
  }

  val taskStr: String = taskOpt.map{
    task =>
      "task " + task.taskAttemptId() +
        {
          var suffix: Seq[String] = Nil
          if (task.isCompleted()) suffix :+= "completed"
          if (task.isInterrupted()) suffix :+= "interrupted"

          if (suffix.isEmpty) ""
          else suffix.mkString("(",",",")")
        }
  }
    .getOrElse("")

  val taskLocationStr: Option[String] = CommonUtils.taskLocationStrOpt

  override def toString = if (taskStr.isEmpty || threadStr.contains(taskStr)) {
    threadStr
  }
  else {
    threadStr + "[" + taskStr + "]"
  }

  /**
    * @return true if any of the thread is dead OR the task is completed
    */
  def isCompleted: Boolean = {
    !thread.isAlive ||
      taskOpt.exists(_.isCompleted())
  }
}