package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext
import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

object TaskOrThreadInfo {

  /**
    * remember not to use in a withDeadline/Future block!
    * @return
    */
  def apply(): TaskOrThreadInfo = {
    val result = Option(TaskContext.get()).map {
      v =>
        TaskInfo(v)
    }
      .getOrElse {
        thread
      }
    result
  }

  def thread: TaskOrThreadInfo = {
    ThreadInfo(java.lang.Thread.currentThread())
  }
}

abstract class TaskOrThreadInfo extends IDMixin {
  def id = _id
  def toEither: Either[TaskContext, java.lang.Thread]
}
case class TaskInfo(self: TaskContext) extends TaskOrThreadInfo {
  override def _id: Any = Left(self.taskAttemptId())
  def toEither = Left(self)

  override def toString: String = "Task-" + self.taskAttemptId()
}
case class ThreadInfo(self: java.lang.Thread) extends TaskOrThreadInfo {
  override def _id: Any = Right(self.getId)
  def toEither = Right(self)

  override def toString: String = "Thread-" + self.getId
}


object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
                      val self: WebDriver,
                      override val taskOrThread: TaskOrThreadInfo = TaskOrThreadInfo()
                    ) extends AutoCleanable {

  def _clean(): Unit = {
    self.close()
    self.quit()
  }
}