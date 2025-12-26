package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Harness
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.{ActionException, ActionExceptionWithCoreDump, SpookyContext, SpookyException}
import com.tribbloids.spookystuff.io.WriteMode.Overwrite
import com.tribbloids.spookystuff.tool.Tool
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * These are the same actions a human would do to get to the data page, their order of execution is identical to that
  * they are defined. Many supports **Cell Interpolation**: you can embed cell reference in their constructor by
  * inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to. This is used almost
  * exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere. extends Product to
  * make sure all subclasses are case classes
  */
@SQLUserDefinedType(udt = classOf[ActionUDT])
trait Action extends Tool[Seq[Observation]] {

  @transient private lazy val _trace = List(this)
  override def trace: Trace = _trace

  var timeElapsed: Long = -1 // only set once

  // execute errorDumps as side effects
  protected def wrapException(
      exception: Exception,
      agent: Harness
  ): ActionException = {

    lazy val backtraceMsg: String = {

      var builder: StringBuilder = new StringBuilder("\n{\n")

      builder
        .append(
          {
            agent.backtrace.map { action =>
              "|  " + action.toString
            } ++
              Seq("+> " + this.detailedStr)
          }
            .mkString("\n")
        )

      builder.append("\n}")
      builder.toString()
    }

    exception match {
      case e: SpookyException.HasCoreDump =>
        val fullMsg = backtraceMsg + e.getMessage_simple
        // do nothing, already dumped
        new ActionException(fullMsg, e)
      case e: SpookyException.HasDoc =>
        val fullMsg = backtraceMsg + "\n" + e.getMessage_simple
        lazy val errorDumpEnabled: Boolean = agent.spooky.conf.errorDump

        if (errorDumpEnabled) {

          // execute coreDump
          val addendum = errorDump(e.doc, agent.spooky)

          new ActionExceptionWithCoreDump(fullMsg + "\n" + e.getClass.getSimpleName + ": " + addendum, e)
        } else {

          new ActionException(fullMsg, e)
        }

      case e =>
        val fullMsg = backtraceMsg + "\n" + e.getMessage
        new ActionException(fullMsg, e)
    }
  }

  // also handle auditing, cache and errorDump
  // TODO: according to RL convention, should only return 1 Observation
  final override def apply(agent: Harness): Seq[Observation] = {

    val results = {
      try {
        exe(agent)
      } catch {
        case e: Exception =>
          def ex = wrapException(e, agent)

//          val ex = e match {
//            case ee: ActionException =>
//              // no need to wrap twice
//              ee
//            case ee: SpookyException.HasDoc =>
//              val doc = ee.doc
//              // invoking errorDump
//              ex
//            case _ =>
//              new ActionException(wrapped, e)
//          }
          throw ex
      }
    }

    this.timeElapsed = System.currentTimeMillis() - agent.startTimeMillis
    agent.spooky.metrics.pagesFetchedFromRemote += results.count(_.isInstanceOf[Doc])

    results
  }

  protected def errorDump(rawDoc: Doc, spooky: SpookyContext): String = {

    val backtrace: Trace =
      if (rawDoc.uid.backtrace.lastOption.exists(_ eq this)) rawDoc.uid.backtrace
      else rawDoc.uid.backtrace :+ this
    val uid = rawDoc.uid.copy(backtrace = backtrace)(nameOvrd = None)
    val doc = rawDoc.copy(uid = uid)(rawDoc.content)
    try {
      doc.prepareSave(spooky, Overwrite).errorDump()
      "saved to: " + doc.saved.last
    } catch {
      case e1: Exception =>
        try {
          doc.prepareSave(spooky, Overwrite).errorDumpLocally()
          s"DFS inaccessible (${e1.toString}) ......... saved to: " + doc.saved.last
        } catch {
          case e2: Exception =>
            s"all file systems inaccessible (${e2.toString}) ......... not saved"
        }
    }
  }

}

object Action {

  trait Driverless extends Action {
    // have not impact to driver, mutually exclusively with MayChangeState

    override val isStateful: Boolean = false
  }

  implicit class _actionOps[T <: Action & Serializable](self: T) {

    def deepCopy(): T = {
      // this wastes a lot of memory, but DSL is not the bulk of memory consumption

      val copy = SerializationUtils.clone(self).asInstanceOf[T]
      copy
    }
  }
}
