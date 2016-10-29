package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext
import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

object TaskThreadInfo {

  /**
    * remember not to use in a withDeadline/Future block!
    * @return
    */
  def apply(): TaskThreadInfo = {
    val result = Option(TaskContext.get()).map {
      v =>
        TaskInfo(v)
    }
      .getOrElse {
        thread
      }
    result
  }

  def thread: TaskThreadInfo = {
    ThreadInfo(java.lang.Thread.currentThread())
  }
}

abstract class TaskThreadInfo extends IDMixin {
  def id = _id
  def toEither: Either[TaskContext, java.lang.Thread]
}
case class TaskInfo(self: TaskContext) extends TaskThreadInfo {
  override def _id: Any = Left(self.taskAttemptId())
  def toEither = Left(self)
}
case class ThreadInfo(self: java.lang.Thread) extends TaskThreadInfo {
  override def _id: Any = Right(self.getId)
  def toEither = Right(self)
}

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

case class CleanWebDriver(
                           self: WebDriver,
                           override val taskOrThread: TaskThreadInfo = TaskThreadInfo()
                         ) extends AutoCleanable {

  def _clean(): Unit = {
    self.close()
    self.quit()
  }
}