package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext
import org.openqa.selenium.NoSuchSessionException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions

trait Cleanable {

  //each can only be cleaned once
  @volatile var isCleaned: Boolean = false

  def logPrefix = ""

  //synchronized to avoid double cleaning
  protected def clean(): Unit = this.synchronized {
    if (!isCleaned){
      cleanImpl()
      isCleaned = true
      LoggerFactory.getLogger(this.getClass).info(s"$logPrefix Cleaned up")
    }
  }

  protected def cleanImpl(): Unit

  def tryClean() = finalize()

  override protected def finalize(): Unit = {
    try {
      clean()
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        val ee = e
        LoggerFactory.getLogger(this.getClass).warn(
          s"$logPrefix !!! FAIL TO CLEAN UP !!!\n" + ee
        )
    }
    finally {
      super.finalize()
    }
  }
}

class Lifespan extends IDMixin {

  type ID = Either[Long, Long]

  def isTask = _id.isLeft
  def isThread = _id.isRight

  def getID: Either[Long, Long] = Option(TaskContext.get()) match {
    case Some(tc) =>
      Lifespan.taskID(tc)
    case None =>
      Lifespan.threadID
  }

  override def toString = _id match {
    case Left(v) => "Task-" + v
    case Right(v) => "Thread-" + v
  }


  @transient lazy val _id = getID
  _id //always getID on construction

  def addCleanupHook(fn: () => Unit): Unit = {
    _id match {
      case Left(_) =>
        TaskContext.get().addTaskCompletionListener {
          tc =>
            fn()
        }
      case Right(_) =>
        sys.addShutdownHook {
          fn()
        }
    }
  }
}

object Lifespan {

  def taskID(tc: TaskContext): Left[Long, Long] = {
    Left(tc.taskAttemptId)
  }

  def threadID: Right[Long, Long] = {
    Right(Thread.currentThread().getId)
  }

  //automatically generates depending on if
  def apply(): Lifespan = Auto()

  case class Auto() extends Lifespan

  case class Task() extends Lifespan {

    require(_id.isLeft, "Not inside any Spark Task")
  }

  case class JVM() extends Lifespan {

    override def getID: ID = threadID
  }
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  * can be serializable, but in which case implementation has to allow deserialized copy on a different machine to be cleanable as well.
  */
trait AutoCleanable extends Cleanable {

  {
    if (!AutoCleanable.uncleaned.contains(lifespan._id)) {
      lifespan.addCleanupHook {
        () =>
          AutoCleanable.cleanup(lifespan._id)
      }
    }
    localUncleaned += this
    LoggerFactory.getLogger(this.getClass).info(s"Creating ${this.getClass.getSimpleName}")
  }

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark
    * Override this to correct such problem
    */
  def lifespan = Lifespan.apply()

  override def logPrefix = {
    s"${lifespan.toString}| ${super.logPrefix}"
  }

  def localUncleaned: ConcurrentSet[AutoCleanable] = {
    AutoCleanable.uncleaned.getOrElseUpdate(
      lifespan._id,
      ConcurrentSet()
    )
  }

  override protected def clean(): Unit = {
    super.clean()
    localUncleaned -= this
  }
}

object AutoCleanable {

  val uncleaned: ConcurrentMap[Lifespan#ID, ConcurrentSet[AutoCleanable]] = ConcurrentMap()

  // cannot execute concurrent
  def cleanup(tt: Lifespan#ID, condition: AutoCleanable => Boolean = _ => true) = {
    val set = uncleaned.getOrElse(tt, mutable.Set.empty)
    val filtered = set.toList
      .filter(condition)
    filtered
      .foreach {
        instance =>
          instance.tryClean()
      }
    set --= filtered
    if (set.isEmpty) uncleaned.remove(tt)
  }

  def cleanupAll(
                  condition: AutoCleanable => Boolean = _ => true
                ) = {

    uncleaned
      .keys
//      .filter(kCondition)
      .foreach {
        tt =>
          cleanup(tt, condition)
      }
  }

  //  def cleanup(tt: Lifespan.ID) = cleanupTyped[AutoCleanable](tt)
  //  def cleanupNotInTask() = {
  //    toBeCleaned
  //      .keys
  //      .filter(_.isRight)
  //      .foreach {
  //        tt =>
  //          cleanup(tt)
  //      }
  //  }
}