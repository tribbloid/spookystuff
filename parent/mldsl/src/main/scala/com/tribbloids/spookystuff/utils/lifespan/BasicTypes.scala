package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.lifespan.Cleanable.{Batch, BatchID}
import com.tribbloids.spookystuff.utils.{CommonUtils, EqualBy}

import scala.util.Try

// Java Deserialization only runs constructor of superclass
// CAUTION: keep the empty constructor in subclasses!
// Without it Kryo deserializer will bypass the hook registration steps in init() when deserializing
trait BasicTypes {

  case object Task extends LeafType {

    case class ID(id: Long) extends EqualBy.Fields {
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

  case object JVM extends LeafType {

    val MAX_NUMBER_OF_SHUTDOWN_HOOKS: Int = CommonUtils.numLocalCores

    case class ID(id: Int) extends EqualBy.Fields {
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

    def delegateTypes: List[LeafType]

    @transient lazy val delegateInstances: List[LeafType#Internal#ForShipping] = {

      delegateTypes.flatMap { v =>
        Try {
          v.apply(nameOpt, ctxFactory)
        }.toOption
      }
    }

    override def children: List[LeafType#Internal] = delegateInstances.map(v => v.value)

    override def _registerBatches_CleanSweepHooks: Seq[(BatchID, Batch)] = {

      delegateInstances.flatMap(_.registeredBatches)
    }
  }

  case class TaskOrJVM(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Compound {
    def this() = this(None)

    override lazy val delegateTypes: List[LeafType] = List(Task, JVM)
  }
}
