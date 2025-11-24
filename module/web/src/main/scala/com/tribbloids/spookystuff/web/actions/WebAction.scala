package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.actions.HasTrace.StateChangeTag
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.web.conf.Web
import com.tribbloids.spookystuff.{ActionException, ActionExceptionWithCoreDump, SpookyException}
import org.slf4j.LoggerFactory

object WebAction {}

trait WebAction extends Action {
  self: StateChangeTag =>

  //    {
  //      Web.enableOnce
  //      // TODO: this no longer works after moving to define-by-run API
  //      //  as constructors are only executed in a function
  //      //  instead, Web module should be initialised in the package object
  //    }

  // execute errorDumps as side effects
  override protected def wrapException(
      exception: Exception,
      agent: Agent
  ): ActionException = {

    val original: ActionException = super.wrapException(exception, agent)

    original match {

      case e: SpookyException.HasCoreDump =>
        e // do nothing, already dumped
      case _ =>
        // execute core dump using snapshot & screenshot from the latest WebDriver
        // even if the original exception doesn't contain any of them

        var msg = original.getMessage_simple

        val driverLookup = agent.getDriver.lookup

        if (driverLookup.contains(Web)) {

          val errorDump: Boolean = agent.spooky.conf.errorDump
          val errorDumpScreenshot: Boolean = agent.spooky.conf.errorScreenshot

          if (errorDump) {
            val rawPage = Snapshot.ErrorDump.exe(agent).head.asInstanceOf[Doc]
            msg += "\nSnapshot: " + this.errorDump(rawPage, agent.spooky)
          }
          if (errorDumpScreenshot) {
            try {
              val rawPage = Screenshot.ErrorScreenshot.exe(agent).head.asInstanceOf[Doc]
              msg += "\nScreenshot: " + this.errorDump(rawPage, agent.spooky)
            } catch {
              case e: Exception =>
                LoggerFactory.getLogger(this.getClass).error("Cannot take screenshot on ActionError:", e)
            }
          }

          new ActionExceptionWithCoreDump(msg, original.getCause)
        } else {

          original
        }
    }
  }
}
