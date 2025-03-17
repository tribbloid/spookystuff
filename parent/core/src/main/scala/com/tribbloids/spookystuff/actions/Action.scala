package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.{ActionException, SpookyContext}
import com.tribbloids.spookystuff.actions.HasTrace.{NoStateChange, StateChangeTag}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.types.SQLUserDefinedType
import org.slf4j.LoggerFactory

/**
  * These are the same actions a human would do to get to the data page, their order of execution is identical to that
  * they are defined. Many supports **Cell Interpolation**: you can embed cell reference in their constructor by
  * inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to. This is used almost
  * exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere. extends Product to
  * make sure all subclasses are case classes
  */
@SQLUserDefinedType(udt = classOf[ActionUDT])
trait Action extends HasTrace {
  self: StateChangeTag =>

  @transient override lazy val trace: Trace = List(this)

  var timeElapsed: Long = -1 // only set once

//  override def dryRun: DryRun = {
//    if (hasExport) {
//      List(List(this))
//    } else {
//      List()
//    }
//  }

  // execute errorDumps as side effects
  protected def getSessionExceptionMessage(
      agent: Agent,
      docOpt: Option[Doc] = None
  ): String = {
    var message: String = "\n{\n"

    message += {
      agent.backtraceBuffer.map { action =>
        "| " + action.toString
      } ++
        Seq("+> " + this.detailedStr)
    }.mkString("\n")

    message += "\n}"

    message
  }

  // also handle auditing, cache and errorDump
  // TODO: according to RL convention, should only return 1 Observation
  final override def apply(agent: Agent): Seq[Observation] = {

    val results = {
      try {
        exe(agent)
      } catch {
        case e: Exception =>
          val message: String = getSessionExceptionMessage(agent)

          val ex = e match {
            case ae: ActionException => ae
            case _                   => new ActionException(message, e)
          }
          throw ex
      }
    }

    this.timeElapsed = System.currentTimeMillis() - agent.startTimeMillis
    agent.spooky.metrics.pagesFetchedFromRemote += results.count(_.isInstanceOf[Doc])

    results
  }

  protected def errorDump(message: String, rawDoc: Doc, spooky: SpookyContext): String = {

    val backtrace: Trace =
      if (rawDoc.uid.backtrace.lastOption.exists(_ eq this)) rawDoc.uid.backtrace
      else rawDoc.uid.backtrace :+ this
    val uid = rawDoc.uid.copy(backtrace = backtrace)(name = null)
    val doc = rawDoc.copy(uid = uid)(rawDoc.content)
    try {
      doc.prepareSave(spooky).errorDump()
      "saved to: " + doc.saved.last
    } catch {
      case _: Exception =>
        try {
          doc.prepareSave(spooky).errorDumpLocally()
          "DFS inaccessible.........saved to: " + doc.saved.last
        } catch {
          case _: Exception =>
            "all file systems inaccessible.........not saved"
        }
    }
  }

  protected[actions] def withTimeoutDuring[T](agent: Agent)(f: => T): T = {

    var baseStr = s"[${agent.taskContextOpt.map(_.partitionId()).getOrElse(0)}]+> ${this.toString}"
    this match {
      case timed: MayTimeout =>
        val timeout = timed.getTimeout(agent)

        baseStr = baseStr + s" in ${timeout}"
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        agent.progress.ping()

        // the following execute f in a different thread, thus `timed` has to be declared as `ThreadSafe`
        CommonUtils.withTimeout(timeout.hardTerimination)(
          f,
          agent.progress.defaultHeartbeat
        )
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        f
    }
  }

  final def exe(agent: Agent): Seq[Observation] = {
    withTimeoutDuring(agent) {
      doExe(agent)
    }
  }

  protected[actions] def doExe(agent: Agent): Seq[Observation]

//  def andThen(f: Seq[Observation] => Seq[Observation]): Action = AndThen(this, f)

//  override def injectFrom(same: ActionLike): Unit = {
//    super.injectFrom(same)
//    this.timeElapsed = same.asInstanceOf[Action].timeElapsed
//  }
}

object Action {

//  trait Placeholder extends Action {
//
//    override protected[actions] def doExe(agent: Agent): Seq[Observation] = {
//      throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} is a placeholder")
//    }
//  }

  trait Driverless extends Action with NoStateChange {
    // have not impact to driver, mutually exclusively with MayChangeState
  }

  implicit class _actionOps[T <: Action](self: T) {

    def deepCopy(): T = {

      val copy = SerializationUtils.clone(self).asInstanceOf[T]
      copy
    }
  }
}
