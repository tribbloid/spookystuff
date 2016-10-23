package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext
import org.openqa.selenium.{NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

import scala.collection.mutable
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

object AutoCleanable {

  lazy val uncleaned: ConcurrentMap[TaskOrThread, ConcurrentSet[AutoCleanable]] = ConcurrentMap()

  def cleanup(tot: TaskOrThread) = {
    val set = uncleaned.getOrElse(tot, mutable.Set.empty)
    val copy = set.toSeq
    copy.foreach {
      instance =>
        instance.finalize()
    }
  }
  def cleanupLocally() = cleanup(TaskOrThread())

  val taskCleanupListener: (TaskContext) => Unit = {
    tc =>
      cleanup(TaskOrThread(Left(tc)))
  }

  def getShutdownHook(thread: Thread): () => Unit = {
    () =>
      cleanup(TaskOrThread(Right(thread)))
  }

  def addListener(v: TaskOrThread): Unit = {
    v.self match {
      case Left(tc) =>
        tc.addTaskCompletionListener(taskCleanupListener)
      case Right(th) =>
      //TODO: add shutdownHook!
    }
  }
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  */
trait AutoCleanable {

  final val taskOrThreadOnCreation: TaskOrThread = TaskOrThread()

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark
    * Override this to correct such problem
    */
  def taskOrThread = taskOrThreadOnCreation

  def localUncleaned: ConcurrentSet[AutoCleanable] = {

    if (!AutoCleanable.uncleaned.contains(taskOrThread)) {
      AutoCleanable.addListener(taskOrThread)
    }
    AutoCleanable.uncleaned.getOrElseUpdate(
      taskOrThread,
      ConcurrentSet()
    )
  }

  localUncleaned += this

  final def clean(): Unit = {
    _clean()
    LoggerFactory.getLogger(this.getClass).info(s"Cleaned up ${this.getClass.getSimpleName}")
    localUncleaned -= this
  }

  def _clean(): Unit

  override def finalize(): Unit = {
    try {
      clean()
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn(s"!!!!! FAIL TO CLEAN UP ${this.getClass.getSimpleName} !!!!!"+e)
    }
    finally {
      super.finalize()
    }
  }
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