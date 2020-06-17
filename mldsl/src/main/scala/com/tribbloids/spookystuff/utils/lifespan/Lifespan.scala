package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin}
import org.apache.spark.TaskContext

import scala.util.Try

/**
  * Java Deserialization only runs constructor of superclass
  */
//CAUTION: keep the empty constructor! Kryo deserializer use them to initialize object
abstract class Lifespan extends IDMixin with Serializable {

  {
    init()
  }
  protected def init(): Unit = {
    ctx
    _id
    //always generate on construction

    if (!Cleanable.uncleaned.contains(_id)) {
      registerHook { () =>
        Cleanable.cleanSweep(_id)
      }
    }
  }

  def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    init() //redundant?
  }

  val ctxFactory: () => LifespanContext
  @transient lazy val ctx: LifespanContext = ctxFactory()

  def getBatchID: Any
  @transient lazy val _id: Any = {
    getBatchID
  }

  def registerHook(
      fn: () => Unit
  ): Unit

  def nameOpt: Option[String]
  override def toString: String = {
    val idStr = Try(_id.toString).getOrElse("[Error]")
    (nameOpt.toSeq ++ Seq(idStr)).mkString(":")
  }

}

object Lifespan {

  abstract class LifespanType extends Serializable with Product {

    // default companion class constructor
    def apply(nameOpt: Option[String] = None, ctxFactory: () => LifespanContext = () => LifespanContext()): Lifespan
  }

  case object Task extends LifespanType {

    case class ID(id: Long) extends AnyVal {
      override def toString: String = s"Task-$id"
    }
  }
  case class Task(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    import Task._

    def task: TaskContext = ctx.task

//    override def tpe: LifespanType = Task
    override def getBatchID: ID = ID(task.taskAttemptId())

    override def registerHook(fn: () => Unit): Unit = {
      task.addTaskCompletionListener[Unit] { _ =>
        fn()
      }
    }
  }

  case object JVM extends LifespanType {

    val MAX_NUMBER_OF_SHUTDOWN_HOOKS: Int = CommonUtils.numLocalCores

    case class ID(id: Int) extends AnyVal {
      override def toString: String = s"Thread-$id"
    }
  }
  case class JVM(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    import JVM._

    override def getBatchID: ID = ID((ctx.thread.getId % JVM.MAX_NUMBER_OF_SHUTDOWN_HOOKS).toInt)

    override def registerHook(fn: () => Unit): Unit =
      try {
        sys.addShutdownHook {
          fn()
        }
      } catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
      }
  }

  case class Compound(
      delegates: Seq[LifespanType],
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(Nil, None)

    {
      delegateInstances
    }

    lazy val delegateInstances: Seq[Lifespan] = {

      delegates.flatMap { v =>
        Try {
          v.apply(nameOpt, ctxFactory)
        }.toOption
      }
    }

    override def getBatchID: Any = {

      delegateInstances.toList.map { ii =>
        Try(ii.getBatchID).toOption
      }
    }

    override def registerHook(fn: () => Unit): Unit = {

      delegateInstances.foreach { ii =>
        try {
          ii.registerHook(fn)
        } catch {
          case e: UnsupportedOperationException => // ignore
        }
      }
    }
  }

  def TaskOrJVM(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ): Compound = Compound(Seq(Task, JVM), nameOpt, ctxFactory)
}
