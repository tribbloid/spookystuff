package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin}
import org.apache.spark.TaskContext

import scala.util.Try

/**
  * Java Deserialization only runs constructor of superclass
  */
//CAUTION: keep the empty constructor in subclasses!
// Without it Kryo deserializer will bypass the hook registration steps in init() when deserializing
abstract class Lifespan extends IDMixin with Serializable {

  {
    init()
  }

  /**
    * should be triggerd on both creation and deserialization
    */
  protected def init(): Unit = {
    ctx
    batchIDs
    //always generate on construction

    batchIDs.foreach { batchID =>
      if (!Cleanable.uncleaned.contains(batchID)) {
        registerHook { () =>
          Cleanable.cleanSweep(batchID)
        }
      }
    }
  }

  def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    init() //redundant?
  }

  val ctxFactory: () => LifespanContext
  @transient lazy val ctx: LifespanContext = ctxFactory()

  def getBatchIDs: Seq[Any]
  @transient final lazy val batchIDs = getBatchIDs
  final protected def _id: Seq[Any] = batchIDs

  def registerHook(
      fn: () => Unit
  ): Unit

  def nameOpt: Option[String]
  override def toString: String = {
    val idStr = Try(batchIDs.mkString("/")).getOrElse("[Error]")
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
    override def getBatchIDs: Seq[ID] = Seq(ID(task.taskAttemptId()))

    override def registerHook(fn: () => Unit): Unit = {
      task.addTaskCompletionListener[Unit] { _ =>
        fn()
      }
    }
  }

  case object JVM extends LifespanType {

    val MAX_NUMBER_OF_SHUTDOWN_HOOKS: Int = CommonUtils.numLocalCores

    case class ID(id: Int) extends AnyVal {
      override def toString: String = s"JVM-$id"
    }
  }
  case class JVM(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    import JVM._

    override def getBatchIDs = Seq(ID((ctx.thread.getId % JVM.MAX_NUMBER_OF_SHUTDOWN_HOOKS).toInt))

    override def registerHook(fn: () => Unit): Unit =
      try {
        sys.addShutdownHook {
          fn()
        }
      } catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
      }
  }

  trait Compound extends Lifespan {

    def delegates: List[LifespanType]

    @transient lazy val delegateInstances: List[Lifespan] = {

      delegates.flatMap { v =>
        Try {
          v.apply(nameOpt, ctxFactory)
        }.toOption
      }
    }

    override def getBatchIDs: Seq[Any] = {

      delegateInstances.flatMap(_.batchIDs)
    }

    override def registerHook(fn: () => Unit): Unit = {}
  }

  case class TaskOrJVM(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Compound {
    def this() = this(None)

    override lazy val delegates: List[LifespanType] = List(Task, JVM)
  }
}
