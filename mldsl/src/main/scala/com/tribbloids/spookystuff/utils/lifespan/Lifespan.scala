package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.TaskContext

import scala.util.Try

/**
  * Java Deserialization only runs constructor of superclass
  */
abstract class Lifespan extends IDMixin with Serializable {

  def tpe: LifespanType
  def ctxFactory: () => LifespanContext

  @transient lazy val ctx = ctxFactory()

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

  {
    init()
  }
  protected def init() = {
    ctx //always generate context on construction

    if (!Cleanable.uncleaned.contains(_id)) {
      tpe.addCleanupHook (
        ctx,
        {
          () =>
            Cleanable.cleanSweep(_id)
        }
      )
    }
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

  def isTask = tpe == Lifespan.Task
}

case class LifespanContext(
                            @transient taskOpt: Option[TaskContext] = Option(TaskContext.get()),
                            @transient thread: Thread = Thread.currentThread()
                          ) extends IDMixin {

  override val _id: Any = taskOpt.map(_.taskAttemptId()) -> thread.getId

  val threadStr: String = {
    "Thread-" + thread.getId + s"[${thread.getName}]" +
      {
        if (thread.isInterrupted) "(interrupted)"
        else if (!thread.isAlive) "(dead)"
        else ""
      }
  }

  val taskStr: String = taskOpt.map{
    task =>
      "Task-" + task.taskAttemptId() +
        {
          var suffix: Seq[String] = Nil
          if (task.isCompleted()) suffix :+= "completed"
          if (task.isInterrupted()) suffix :+= "interrupted"
          //          if (task.isRunningLocally()) suffix :+= "local"
          suffix.mkString("(",",",")")
        }
  }
    .getOrElse("[NOT IN TASK]")

  override def toString = threadStr + " / " + taskStr
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

    private def tc(ctx: LifespanContext) = {
      ctx.taskOpt.getOrElse(
        throw new UnsupportedOperationException("Not inside any Spark Task")
      )
    }

    override def addCleanupHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      tc(ctx).addTaskCompletionListener {
        tc =>
          fn()
      }
    }

    case class ID(id: Long) {
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
      }
      catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
      }
    }

    case class ID(id: Long) {
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

  object Auto extends LifespanType {
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
  case class Auto(
                   override val nameOpt: Option[String] = None,
                   ctxFactory: () => LifespanContext = () => LifespanContext()
                 ) extends Lifespan {
    def this() = this(None)

    override def tpe: LifespanType = Auto
  }
}
