package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import org.apache.spark.TaskContext
import org.openqa.selenium.NoSuchSessionException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait Cleanable {

  //each can only be cleaned once
  @volatile var isCleaned: Boolean = false

  protected def cleanImpl(): Unit = {
    if (!isCleaned){
      _cleanImpl()
    }
    isCleaned = true
    LoggerFactory.getLogger(this.getClass).info(s"Cleaned up ${this.getClass.getSimpleName}")
  }

  protected def _cleanImpl(): Unit

  def clean() = finalize()

  override protected def finalize(): Unit = {
    try {
      cleanImpl()
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        val ee = e
        LoggerFactory.getLogger(this.getClass).warn(
          s"!!! FAIL TO CLEAN UP ${this.getClass.getName} !!!" + e
        )
    }
    finally {
      super.finalize()
    }
  }
}

abstract class Lifespan {

  protected def getID: Lifespan.ID

  @transient lazy val _id = getID
  _id //always getID on construction

  def addCleanupHook(fn: () => Unit): Unit
}

class LifespanImpl extends Lifespan {

  override def getID = Option(TaskContext.get()) match {
    case Some(tc) =>
      Lifespan.left(tc)
    case None =>
      Lifespan.right
  }

  override def toString = _id match {
    case Left(v) => "Task-" + v
    case Right(v) => "Thread-" + v
  }

  override def addCleanupHook(fn: () => Unit): Unit = {
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

  type ID = Either[Long,Long]

  def left(tc: TaskContext): Left[Long, Long] = {
    Left(tc.taskAttemptId)
  }

  def right: Right[Long, Long] = {
    Right(Thread.currentThread().getId)
  }

  //automatically generates depending on if
  def apply(): Lifespan = Auto()

  case class Auto() extends LifespanImpl

  case class Task() extends LifespanImpl {

    require(_id.isLeft, "Not inside any Spark Task")
  }

  case class JVM() extends LifespanImpl {

    override def getID: ID = right
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
    if (!AutoCleanable.toBeCleaned.contains(lifespan._id)) {
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

  def localUncleaned: ConcurrentSet[AutoCleanable] = {
    AutoCleanable.toBeCleaned.getOrElseUpdate(
      lifespan._id,
      ConcurrentSet()
    )
  }

  override protected def cleanImpl(): Unit = {
    super.cleanImpl()
    localUncleaned -= this
  }
}

object AutoCleanable {

  val toBeCleaned: ConcurrentMap[Lifespan.ID, ConcurrentSet[AutoCleanable]] = ConcurrentMap()

  def cleanupTyped[T <: AutoCleanable : ClassTag](tt: Lifespan.ID) = {
    val set = toBeCleaned.getOrElse(tt, mutable.Set.empty)
    val filtered = set.toList
      .collect {
        case v: T => v
      }
    filtered
      .foreach {
        instance =>
          instance.clean()
      }
    set --= filtered
    if (set.isEmpty) toBeCleaned.remove(tt)
  }

  def cleanupAllTyped[T <: AutoCleanable: ClassTag]() = {
    toBeCleaned.keys.toList.foreach {
      tt =>
        cleanupTyped[T](tt)
    }
  }

  def cleanup(tt: Lifespan.ID) = cleanupTyped[AutoCleanable](tt)
  def cleanupNotInTask() = {
    toBeCleaned
      .keys
      .filter(_.isRight)
      .foreach {
        tt =>
          cleanup(tt)
      }
  }
}