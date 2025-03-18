package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.actions.HasTrace.StateChangeTag
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.web.conf.Web
import org.slf4j.LoggerFactory

object WebAction {}

trait WebAction extends Action {
  self: StateChangeTag =>

  {
    Web.enableOnce
  }

  // execute errorDumps as side effects
  override protected def getSessionExceptionMessage(
      agent: Agent,
      docOpt: Option[Doc] = None
  ): String = {

    var message = super.getSessionExceptionMessage(agent, docOpt)

    lazy val errorDump: Boolean = agent.spooky.conf.errorDump
    lazy val errorDumpScreenshot: Boolean = agent.spooky.conf.errorScreenshot

    agent match {
      case d: Agent =>
        if (d.Drivers.lookup.get(Web).nonEmpty) {
          if (errorDump) {
            val rawPage = Snapshot.ErrorDump.exe(agent).head.asInstanceOf[Doc]
            message += "\nSnapshot: " + this.errorDump(message, rawPage, agent.spooky)
          }
          if (errorDumpScreenshot) {
            try {
              val rawPage = Screenshot.ErrorScreenshot.exe(agent).head.asInstanceOf[Doc]
              message += "\nScreenshot: " + this.errorDump(message, rawPage, agent.spooky)
            } catch {
              case e: Exception =>
                LoggerFactory.getLogger(this.getClass).error("Cannot take screenshot on ActionError:", e)
            }
          }
        } else {
          docOpt.foreach { doc =>
            if (errorDump) {
              message += "\nSnapshot: " + this.errorDump(message, doc, agent.spooky)
            }
          }
        }
    }
    message
  }
}
