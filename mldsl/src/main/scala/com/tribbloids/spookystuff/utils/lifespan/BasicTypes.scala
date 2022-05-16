package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.lifespan.Cleanable.{BatchID, Lifespan}
import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin}

import scala.util.Try

// Java Deserialization only runs constructor of superclass
// CAUTION: keep the empty constructor in subclasses!
// Without it Kryo deserializer will bypass the hook registration steps in init() when deserializing
trait BasicTypes {

  case object Task extends ElementaryType {

    case class ID(id: Long) extends IDMixin.ForProduct {
      override def toString: String = s"Task-$id"
    }

    override protected def _batchID(ctx: LifespanContext): ID =
      ID(ctx.task.taskAttemptId())

    override protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      ctx.task.addTaskCompletionListener[Unit] { _ =>
        fn()
      }
    }

  }

  case object JVM extends ElementaryType {

    val MAX_NUMBER_OF_SHUTDOWN_HOOKS: Int = CommonUtils.numLocalCores

    case class ID(id: Int) extends IDMixin.ForProduct {
      override def toString: String = s"JVM-$id"
    }

    override protected def _batchID(ctx: LifespanContext): ID =
      ID((ctx.thread.getId % MAX_NUMBER_OF_SHUTDOWN_HOOKS).toInt)

    override protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      try {
        sys.addShutdownHook {
          fn()
        }
      } catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
      }
    }
  }

  trait Compound extends LifespanInternal {

    def delegates: List[ElementaryType]

    @transient lazy val delegateInstances: List[Lifespan] = {

      delegates.flatMap { v =>
        Try {
          v(nameOpt, ctxFactory)
        }.toOption
      }
    }

    override def _register: Seq[BatchID] = {

      delegateInstances.flatMap(_.registeredID)
    }
  }

  case class TaskOrJVM(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Compound {
    def this() = this(None)

    override lazy val delegates: List[ElementaryType] = List(Task, JVM)
  }
}
