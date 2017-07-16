package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.utils.{NOTSerializable, TreeException}
import org.openqa.selenium.NoSuchSessionException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

sealed trait AbstractCleanable {

  logConstructionDestruction("Created")

  //each can only be cleaned once
  @volatile var isCleaned: Boolean = false

  def logPrefix: String
  protected def logConstructionDestruction(s: String) = {
    LoggerFactory.getLogger(this.getClass).debug(s"$logPrefix $s")
  }

  protected def cleanImpl(): Unit

  //  private object CleanupLock
  //avoid double cleaning, this lock is not shared with any other invocation, PARTICULARLY subclasses
  def clean(silent: Boolean = false): Unit = {
    if (!isCleaned){
      isCleaned = true
      try {
        cleanImpl()
        if (!silent) logConstructionDestruction("Destroyed")
      }
      catch {
        case e: Throwable =>
          isCleaned = false
          throw e
      }
    }
  }

  def tryClean(silent: Boolean = false): Unit = {
    try {
      clean(silent)
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        val ee = e
        LoggerFactory.getLogger(this.getClass).warn(
          s"$logPrefix !!! FAIL TO CLEAN UP !!!\n", ee
        )
    }
    finally {
      super.finalize()
    }
  }

  override protected def finalize() = tryClean()
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  * can be serializable, but in which case implementation has to allow deserialized copy on a different machine to be cleanable as well.
  */
trait Cleanable extends AbstractCleanable {

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark
    * Override this to correct such problem
    */
  def lifespan: Lifespan = new Lifespan.JVM()
  lifespan //initialize lazily

  uncleanedInBatch += this

  override def logPrefix = {
    s"${lifespan.toString} \t| "
  }

  @transient lazy val uncleanedInBatch: ConcurrentSet[Cleanable] = {
    // This weird implementation is to mitigate thread-unsafe competition:
    // 2 empty Set being inserted simultaneously
    Cleanable.uncleaned
      .getOrElseUpdate(
        lifespan._id,
        Cleanable.synchronized{
          Cleanable.uncleaned.getOrElse(
            lifespan._id,
            ConcurrentSet()
          )
        }
      )
  }

  def subCleanable: Seq[Cleanable] = Nil

  override def clean(silent: Boolean): Unit = {
    val trials: Seq[Try[Unit]] = subCleanable.map {
      v =>
        Try {
          v.clean(silent)
        }
    }
    TreeException.&&&(trials :+ Try(super.clean(silent)))

    uncleanedInBatch -= this
  }
}

object Cleanable {

  val uncleaned: ConcurrentMap[Any, ConcurrentSet[Cleanable]] = ConcurrentMap()

  def getByLifespan(tt: Any, condition: (Cleanable) => Boolean): (ConcurrentSet[Cleanable], List[Cleanable]) = {
    val set = uncleaned.getOrElse(tt, mutable.Set.empty)
    val filtered = set.toList //create deep copy to avoid in-place deletion
      .filter(condition)
    (set, filtered)
  }
  def getAll(
              condition: (Cleanable) => Boolean = _ => true
            ): Seq[Cleanable] = {
    uncleaned
      .values.toList
      .flatten
      .filter(condition)
  }
  def getTyped[T <: Cleanable: ClassTag]: Seq[T] = {
    val result = getAll{
      case v: T => true
      case _ => false
    }
      .map { v =>
        v.asInstanceOf[T]
      }
    result
  }

  // cannot execute concurrent
  def cleanSweep(
                  tt: Any,
                  condition: Cleanable => Boolean = _ => true
                ) = {
    val (set: ConcurrentSet[Cleanable], filtered: List[Cleanable]) = getByLifespan(tt, condition)
    filtered
      .foreach {
        instance =>
          instance.tryClean()
      }
    set --= filtered
    if (set.isEmpty) uncleaned.remove(tt)
  }

  def cleanSweepAll(
                     condition: Cleanable => Boolean = _ => true
                   ) = {

    uncleaned
      .keys.toList
      .foreach {
        tt =>
          cleanSweep(tt, condition)
      }
  }

//  trait Verbose {
//    self: Cleanable =>
//
//    override def logConstructionDestruction(s: String): Unit = {
//      LoggerFactory.getLogger(this.getClass).info(s"$logPrefix $s")
//    }
//  }
}

trait LocalCleanable extends Cleanable with NOTSerializable