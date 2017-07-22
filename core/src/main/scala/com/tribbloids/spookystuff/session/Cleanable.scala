package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.utils.{NOTSerializable, TreeException}
import org.openqa.selenium.NoSuchSessionException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Random, Try}

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
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  * can be serializable, but in which case implementation has to allow deserialized copy on a different machine to be cleanable as well.
  */
trait Cleanable {

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark
    * Override this to correct such problem
    */
  def _lifespan: Lifespan = new Lifespan.JVM()
  final val lifespan = _lifespan
  final val trackingNumber = Random.nextInt()

  //each can only be cleaned once
  @volatile var isCleaned: Boolean = false
  @volatile var stacktraceAtCleaning: Option[Array[StackTraceElement]] = None

  {
    logPrefixed("Created")
    uncleanedInBatch += this
  }

  def logPrefix: String = {
    s"$trackingNumber @ ${lifespan.toString} \t| "
  }
  protected def logPrefixed(s: String) = {
    LoggerFactory.getLogger(this.getClass).info(s"$logPrefix $s")
  }

  protected def cleanImpl(): Unit

  def assertNotCleaned(errorInfo: String): Unit = {
    assert(
      !isCleaned,
      s"$logPrefix $errorInfo: $this is already cleaned @\n" +
        s"${stacktraceAtCleaning.get.mkString("\n")}"
    )
  }

  def uncleanedInBatch: ConcurrentSet[Cleanable] = {
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

  def clean(silent: Boolean = false): Unit = {
    val sub: Seq[Try[Unit]] = subCleanable.map {
      v =>
        Try {
          v.clean(silent)
        }
    }
    val self = Try{
      if (!isCleaned){
        isCleaned = true
        stacktraceAtCleaning = Some(Thread.currentThread().getStackTrace)
        try {
          cleanImpl()
          if (!silent) logPrefixed("Cleaned")
        }
        catch {
          case e: Throwable =>
            isCleaned = false
            stacktraceAtCleaning = None
            throw e
        }
      }
    }
    TreeException.&&&(sub :+ self)

    uncleanedInBatch -= this
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

trait LocalCleanable extends Cleanable with NOTSerializable