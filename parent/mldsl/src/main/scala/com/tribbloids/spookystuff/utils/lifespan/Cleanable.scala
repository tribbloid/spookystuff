package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.Caching._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions
import scala.ref.WeakReference
import scala.reflect.ClassTag

object Cleanable {

  import com.tribbloids.spookystuff.utils.CommonViews._

  type Lifespan = LifespanInternal#ForShipping
  type Leaf = LeafType#Internal

  // Java Deserialization only runs constructor of superclass
  object Lifespan extends BasicTypes with HadoopTypes with SparkTypes

  type BatchID = LeafType#ID
  type Batch = ConcurrentMap[Long, WeakReference[Cleanable]] // trackingNumber -> instance
  lazy val uncleaned: ConcurrentMap[BatchID, Batch] = ConcurrentMap()

  case class Select[T <: Cleanable: ClassTag](
      ids: Seq[BatchID],
      condition: T => Boolean = (_: T) => true
  ) {

    final def batches: Seq[Batch] = ids.flatMap { id =>
      Select1Batch(id).get
    }

    final def cleanables: Seq[T] = batches
      .flatMap(_.values)
      .flatMap(_.get)
      .collect {
        case v: T => v
      }
      .filter(condition)

    def filter(condition: Cleanable => Boolean = _ => true): Select[T] = {

      this.copy(condition = condition)
    }

    def typed[R <: T: ClassTag]: Select[R] = {
      this.copy[R]()
    }

    def cleanSweep(): Unit = {

      val filtered = cleanables

      filtered
        .foreach { instance =>
          instance.tryClean()
        }

      ids.foreach { id =>
        batches.foreach { batch =>
          if (batch.isEmpty) uncleaned.remove(id)
        }
      }
    }
  }

//  def batchOf(id: BatchID): Selection[Cleanable] = Selection(Seq(id))

  def All: Select[Cleanable] = Select[Cleanable](uncleaned.keys.toSeq)

  case class Select1Batch(id: BatchID) {

    def getOrExecute(exe: () => Batch): Batch = uncleaned.getOrElseUpdateSynchronously(id) {

      exe()
    }

    def get: Option[Batch] = uncleaned.get(id)

    @deprecated // creating a batch without registering clean sweep hook is illegal
    def getOrCreate: Batch = getOrExecute { () =>
      ConcurrentMap()
    }
  }

  implicit def selectBatchAsSelect(v: Select1Batch): Select[Cleanable] = Select(Seq(v.id))
}

/**
  * This is a trait that unifies resource cleanup on both Spark Driver & Executors instances created on Executors are
  * cleaned by Spark TaskCompletionListener instances created otherwise are cleaned by JVM shutdown hook finalizer helps
  * but is not always reliable can be serializable, but in which case implementation has to allow deserialized copy on a
  * different machine to be cleanable as well.
  */
trait Cleanable extends AutoCloseable {

  import Cleanable._

  /**
    * taskOrThreadOnCreation is incorrect in withDeadline or threads not created by Spark Override this to correct such
    * problem
    */
  def _lifespan: Lifespan = Lifespan.JVM()
  final val lifespan: Lifespan = _lifespan
  final val trackingNumber: Long = System.identityHashCode(this).toLong // can be int value

  override def finalize(): Unit = {
    tryClean()
  }

  // TODO: useless, blocked by https://stackoverflow.com/questions/77290708/in-java-9-with-scala-how-to-make-a-cleanable-that-can-be-triggered-by-system
//  @transient final private lazy val cleanable: Cleaner.Cleanable = jvmCleaner.register(
//    this,
//    () => tryClean()
//  )

  {
    logPrefixed("Created")
    batches.foreach { inBatch =>
      inBatch += this.trackingNumber -> WeakReference(this)
    }
//    cleanable // actually eager execution on creation
  }

  // each can only be cleaned once
  @volatile protected var _isCleaned: Boolean = false
  def isCleaned: Boolean = this.synchronized {
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

  lazy val doCleanOnce: Unit = this.synchronized {

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
    }
  }

  final override def close(): Unit = {
    clean(true)
  }
}
