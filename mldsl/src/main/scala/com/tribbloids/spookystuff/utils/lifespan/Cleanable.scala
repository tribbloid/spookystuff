package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.CachingUtils._
import org.slf4j.{Logger, LoggerFactory}

import java.io.Closeable
import scala.reflect.ClassTag

object Cleanable {

  case class StateLock()

  import com.tribbloids.spookystuff.utils.CommonViews._

  type Lifespan = LifespanInternal#ForShipping
  type Leaf = LeafType#Internal

  // Java Deserialization only runs constructor of superclass
  object Lifespan extends BasicTypes with HadoopTypes with SparkTypes

  type BatchID = LeafType#ID
  type Batch = ConcurrentMap[Long, Cleanable] // trackingNumber -> instance
  lazy val uncleaned: ConcurrentMap[BatchID, Batch] = ConcurrentMap()

  trait Selection {

    def ids: Seq[BatchID]

    final def batches: Seq[Batch] = ids.flatMap { id =>
      Select(id).get
    }

    def filter(condition: Cleanable => Boolean = _ => true): Seq[Cleanable] = {

      batches.flatMap(batch => batch.values).filter(condition)
    }

    def typed[T <: Cleanable: ClassTag]: Seq[T] = {
      val result = filter {
        case _: T => true
        case _    => false
      }.map { v =>
        v.asInstanceOf[T]
      }

      result
    }

    def cleanSweep(condition: Cleanable => Boolean = _ => true): Unit = {

      ids.foreach { id =>
        Select(id).get.foreach { batch =>
          val filtered = batch.values.filter(condition)

          filtered
            .foreach { instance =>
              instance.tryClean()
            }
          if (batch.isEmpty) uncleaned.remove(id)
        }
      }
    }
  }

  case class Select(id: BatchID) extends Selection {
    override def ids: Seq[BatchID] = Seq(id)

    def getOrExecute(exe: () => Batch): Batch = uncleaned.getOrElseUpdateSynchronously(id) {

      exe()
    }

    def get: Option[Batch] = uncleaned.get(id)

    @deprecated // creating a batch without registering clean sweep hook is illegal
    def getOrCreate: Batch = getOrExecute { () =>
      ConcurrentMap()
    }
  }

  case object All extends Selection {
    override def ids: Seq[BatchID] = uncleaned.keys.toSeq
  }
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors
  * instances created on Executors are cleaned by Spark TaskCompletionListener
  * instances created otherwise are cleaned by JVM shutdown hook
  * finalizer helps but is not always reliable
  * can be serializable, but in which case implementation has to allow deserialized copy on a different machine to be cleanable as well.
  */
trait Cleanable extends Closeable {

  import Cleanable._

  @transient lazy val stateLock: StateLock = StateLock()

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark
    * Override this to correct such problem
    */
  def _lifespan: Lifespan = Lifespan.JVM()
  final val lifespan = _lifespan
  final val trackingNumber = System.identityHashCode(this).toLong // can be int value

  {
    logPrefixed("Created")
    batches.foreach { inBatch =>
      inBatch += this.trackingNumber -> this
    }
  }

  //each can only be cleaned once
  @volatile protected var _isCleaned: Boolean = false
  def isCleaned: Boolean = stateLock.synchronized {
    _isCleaned
  }

  @volatile var stacktraceAtCleaning: Option[Array[StackTraceElement]] = None

  @transient lazy val batches: Seq[Batch] = {
    // This weird implementation is to mitigate thread-unsafe competition:
    // 2 empty collections being inserted simultaneously
    lifespan.registeredBatches.map { v =>
      v._2
    }
  }

  def logPrefix: String = {
    s"$trackingNumber @ ${lifespan.toString} \t| "
  }

  protected def cleanableLogFunction(logger: Logger): String => Unit = {
    logger.debug
  }

  protected def logPrefixed(s: String): Unit = {
    cleanableLogFunction(LoggerFactory.getLogger(this.getClass))
      .apply(s"$logPrefix $s")
  }

  /**
    * can only be called once
    */
  protected def cleanImpl(): Unit

  def assertNotCleaned(errorInfo: String): Unit = {
    assert(
      !isCleaned,
      s"$logPrefix $errorInfo: $this is already cleaned @\n" +
        s"${stacktraceAtCleaning.get.mkString("\n")}"
    )
  }

  lazy val doCleanOnce: Unit = stateLock.synchronized {

    stacktraceAtCleaning = Some(Thread.currentThread().getStackTrace)
    try {
      cleanImpl()
      _isCleaned = true

      batches.foreach { inBatch =>
        inBatch -= this.trackingNumber
      }
    } catch {
      case e: Throwable =>
        stacktraceAtCleaning = None
        throw e
    }
  }

  final def clean(silent: Boolean = false): Unit = {

    val isCleaned = this.isCleaned
    doCleanOnce

    if (!silent && !isCleaned) logPrefixed("Cleaned")
  }

  def silentOnError(ee: Throwable): Boolean = false

  final def tryClean(silent: Boolean = false): Unit = {
    try {
      clean(silent)
    } catch {
      case e: Exception =>
        val ee = e
        if (!silentOnError(ee))
          LoggerFactory
            .getLogger(this.getClass)
            .error(
              s"$logPrefix !!! FAILED TO CLEAN UP !!!\n",
              ee
            )
    } finally {
      super.finalize()
    }
  }

  override protected def finalize(): Unit = tryClean()

  final override def close(): Unit = clean()
}
