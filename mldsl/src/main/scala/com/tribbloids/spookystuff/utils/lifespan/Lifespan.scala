package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Java Deserialization only runs constructor of superclass
  */
abstract class Lifespan extends IDMixin with Serializable {

  {
    init()
  }
  protected def init(): Unit = {
    ctx //always generate context on construction

    if (!Cleanable.uncleaned.contains(_id)) {
      tpe.addCleanupHook(
        ctx, { () =>
          Cleanable.cleanSweep(_id)
        }
      )
    }
  }

  def tpe: LifespanType
  def ctxFactory: () => LifespanContext

  @transient lazy val ctx: LifespanContext = ctxFactory()

  //  @transient @volatile var _ctx: LifespanContext = _
  //  def ctx = this.synchronized {
  //    if (_ctx == null) {
  //      updateCtx()
  //    }
  //    else {
  //      _ctx
  //    }
  //  }
  //
  //  def updateCtx(factory: () => LifespanContext = ctxFactory): LifespanContext = {
  //    val neo = ctxFactory()
  //    _ctx = neo
  //    neo
  //  }

  def _id: Any = {
    tpe.getCleanupBatchID(ctx)
  }

  def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    init() //redundant?
  }

  def nameOpt: Option[String]
  override def toString: String = {
    val idStr = Try(_id.toString).getOrElse("[Error]")
    (nameOpt.toSeq ++ Seq(idStr)).mkString(":")
  }

  def isTask: Boolean = tpe == Lifespan.Task
}

abstract class LifespanType extends Serializable {

  def addCleanupHook(
      ctx: LifespanContext,
      fn: () => Unit
  ): Unit

  def getCleanupBatchID(ctx: LifespanContext): Any
}

object Lifespan {

  object Task extends LifespanType {

    private def getTaskContext(ctx: LifespanContext) = ctx.task

    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      getTaskContext(ctx).addTaskCompletionListener[Unit] { tc =>
        fn()
      }
    }

    case class ID(id: Long) extends AnyVal {
      override def toString: String = s"Task-$id"
    }
    override def getCleanupBatchID(ctx: LifespanContext): ID = {
      ID(getTaskContext(ctx).taskAttemptId())
    }
  }
  case class Task(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    override def tpe: LifespanType = Task
  }

  object JVM extends LifespanType {
    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      try {
        sys.addShutdownHook {
          fn()
        }
      } catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
      }
    }

    case class ID(id: Long) extends AnyVal {
      override def toString: String = s"Thread-$id"
    }
    override def getCleanupBatchID(ctx: LifespanContext): ID = {
      ID(ctx.thread.getId)
    }
  }
  case class JVM(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    override def tpe: LifespanType = JVM
  }

  object SparkApp extends LifespanType {

    {
      sc
    }

    lazy val sc: SparkContext = SparkSession.active.sparkContext

    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {

      sc.addSparkListener(new Listener(fn))
    }

    override def getCleanupBatchID(ctx: LifespanContext): Any = {
      sc.applicationId
    }

    class Listener(fn: () => Unit) extends SparkListener with Serializable {

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

        fn()
      }
    }
  }
  case class SparkApp(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    override def tpe: LifespanType = SparkApp
  }

  //CAUTION: keep the empty constructor! Kryo deserializer use them to initialize object
  case class Compound(
      delegates: Seq[LifespanType],
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(Nil, None)

    override def tpe: LifespanType = CompoundType

    object CompoundType extends LifespanType {

      override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {

        delegates.foreach { delegate =>
          try {
            delegate.addCleanupHook(ctx, fn)
          } catch {
            case e: UnsupportedOperationException => // ignore
          }
        }
      }

      override def getCleanupBatchID(ctx: LifespanContext): Any = {

        delegates.toList.map { delegate =>
          Try(delegate.getCleanupBatchID(ctx)).toOption
        }
      }
    }
  }

  def TaskOrJVM(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ): Compound = Compound(Seq(Task, JVM), nameOpt, ctxFactory)
}
