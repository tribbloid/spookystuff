package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import org.apache.spark.TaskContext
import org.openqa.selenium.NoSuchSessionException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions

trait Cleanable {

  @volatile var isCleaned: Boolean = false

  protected def clean(): Unit = {
    if (!isCleaned){
      _clean()
    }
    isCleaned = true
    LoggerFactory.getLogger(this.getClass).info(s"Cleaned up ${this.getClass.getSimpleName}")
  }

  protected def _clean(): Unit

  override def finalize(): Unit = {
    try {
      clean()
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        val ee = e
        LoggerFactory.getLogger(this.getClass).warn(
          s"!!! FAIL TO CLEAN UP ${this.getClass.getName} !!!"+e
        )
    }
    finally {
      super.finalize()
    }
  }
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  */
trait AutoCleanable extends Cleanable {

  final val taskOrThreadOnCreation: TaskOrThreadInfo = TaskOrThreadInfo()

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

  override protected def clean(): Unit = {
    super.clean()
    localUncleaned -= this
  }

  localUncleaned += this
  LoggerFactory.getLogger(this.getClass).info(s"Creating ${this.getClass.getSimpleName}")
}

object AutoCleanable {

  lazy val uncleaned: ConcurrentMap[TaskOrThreadInfo, ConcurrentSet[AutoCleanable]] = ConcurrentMap()

  def cleanup(tt: TaskOrThreadInfo) = {
    val set = uncleaned.getOrElse(tt, mutable.Set.empty)
    val copy = set.toSeq
    copy.foreach {
      instance =>
        instance.finalize()
    }
  }
  def cleanupLocally() = cleanup(TaskOrThreadInfo())

  val taskCleanupListener: (TaskContext) => Unit = {
    tc =>
      cleanup(TaskInfo(tc))
  }

  def getShutdownHook(thread: Thread) = new Thread {
    override def run() = {
      cleanup(ThreadInfo(thread))
    }
  }

  def addListener(v: TaskOrThreadInfo): Unit = {
    v match {
      case TaskInfo(tc) =>
        tc.addTaskCompletionListener(taskCleanupListener)
      case ThreadInfo(th) =>
        Runtime.getRuntime.addShutdownHook(
          getShutdownHook(th)
        )
    }
  }
}