package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext
import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

object TaskOrThread {

  /**
    * remember not to use in a withDeadline/Future block!
    * @return
    */
  def apply(): TaskOrThread = {
    val self = Option(TaskContext.get()).map {
      v =>
        Left(v)
    }
      .getOrElse {
        Right(Thread.currentThread())
      }
    TaskOrThread(self)
  }
}

// should be treated as the same thing, to allow local or remote cleanup.
case class TaskOrThread(
                         self: Either[TaskContext, Thread]
                       ) extends IDMixin {

  val _id: Either[Long, Long] = self match {
    case Left(v) => Left(v.taskAttemptId())
    case Right(v) => Right(v.getId)
  }

  def id = _id
}

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

case class CleanWebDriver(
                           self: WebDriver,
                           override val taskOrThread: TaskOrThread
                         ) extends AutoCleanable {

  def _clean(): Unit = {
    self.close()
    self.quit()
  }
}