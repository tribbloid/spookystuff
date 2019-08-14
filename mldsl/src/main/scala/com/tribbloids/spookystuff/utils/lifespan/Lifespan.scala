package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin

import scala.util.Try

/**
  * Java Deserialization only runs constructor of superclass
  */
abstract class Lifespan extends IDMixin with Serializable {

  {
    init()
  }
  protected def init() = {
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

  def _id = {
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

    private def tc(ctx: LifespanContext) = ctx.task

    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      tc(ctx).addTaskCompletionListener[Unit] { tc =>
        fn()
      }
    }

    case class ID(id: Long) extends AnyVal {
      override def toString: String = s"Task-$id"
    }
    override def getCleanupBatchID(ctx: LifespanContext): ID = {
      ID(tc(ctx).taskAttemptId())
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

  object TaskOrJVM extends LifespanType {
    private def delegate(ctx: LifespanContext) = {
      ctx.taskOpt match {
        case Some(tc) =>
          Task
        case None =>
          JVM
      }
    }

    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      delegate(ctx).addCleanupHook(ctx, fn)
    }

    override def getCleanupBatchID(ctx: LifespanContext): Any = {
      delegate(ctx).getCleanupBatchID(ctx)
    }
  }
  //CAUTION: keep the empty constructor! Kryo deserializer use them to initialize object
  case class TaskOrJVM(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    override def tpe: LifespanType = TaskOrJVM
  }
}
