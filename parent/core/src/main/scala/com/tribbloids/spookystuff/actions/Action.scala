package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Trace.DryRun
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.{ActionException, SpookyContext}
import org.apache.spark.sql.types.SQLUserDefinedType
import org.slf4j.LoggerFactory

/**
  * These are the same actions a human would do to get to the data page, their order of execution is identical to that
  * they are defined. Many supports **Cell Interpolation**: you can embed cell reference in their constructor by
  * inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to. This is used almost
  * exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere. extends Product to
  * make sure all subclasses are case classes
  */
//TODO: merging with Extractor[Seq[Fetched]]?
@SQLUserDefinedType(udt = classOf[ActionUDT])
trait Action extends ActionLike with HasTrace {

  override def children: Trace = Nil
  @transient override lazy val asTrace: Trace = List(this)

  var timeElapsed: Long = -1 // only set once

  override def dryRun: DryRun = {
    if (hasOutput) {
      List(List(this))
    } else {
      List()
    }
  }

  // execute errorDumps as side effects
  protected def getSessionExceptionMessage(
      agent: Agent,
      docOpt: Option[Doc] = None
  ): String = {
    var message: String = "\n{\n"

    message += {
      agent.backtrace.map { action =>
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

    val results =
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

    this.timeElapsed = System.currentTimeMillis() - agent.startTimeMillis
    agent.spooky.spookyMetrics.pagesFetchedFromRemote += results.count(_.isInstanceOf[Doc])

    results
  }

  protected def errorDump(message: String, rawDoc: Doc, spooky: SpookyContext): String = {

    val backtrace: Trace =
      if (rawDoc.uid.backtrace.lastOption.exists(_ eq this)) rawDoc.uid.backtrace
      else rawDoc.uid.backtrace :+ this
    val uid = rawDoc.uid.copy(backtrace = backtrace)(name = null)
    val doc = rawDoc.copy(uid = uid)(rawDoc.content)
    try {
      doc.save(spooky).errorDump()
      "saved to: " + doc.saved.last
    } catch {
      case _: Exception =>
        try {
          doc.save(spooky).errorDumpLocally()
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
      case timed: Timed.ThreadSafe =>
        baseStr = baseStr + s" in ${timed.timeout(agent)}"
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        agent.progress.ping()

        // the following execute f in a different thread, thus `timed` has to be declared as `ThreadSafe`
        CommonUtils.withTimeout(timed.hardTerminateTimeout(agent))(
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

  protected def doExe(agent: Agent): Seq[Observation]

  def andThen(f: Seq[Observation] => Seq[Observation]): Action = AndThen(this, f)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.timeElapsed = same.asInstanceOf[Action].timeElapsed
  }
}
