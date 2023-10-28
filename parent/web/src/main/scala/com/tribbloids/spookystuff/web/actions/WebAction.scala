package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.web.conf.Web
import org.slf4j.LoggerFactory

object WebAction {}

trait WebAction extends Action {

  {
    Web.enableOnce
  }

  // execute errorDumps as side effects
  override protected def getSessionExceptionMessage(
      session: Session,
      docOpt: Option[Doc] = None
  ): String = {

    var message = super.getSessionExceptionMessage(session, docOpt)

    lazy val errorDump: Boolean = session.spooky.spookyConf.errorDump
    lazy val errorDumpScreenshot: Boolean = session.spooky.spookyConf.errorScreenshot

    session match {
      case d: Session =>
        if (d.Drivers.get(Web).nonEmpty) {
          if (errorDump) {
            val rawPage = Snapshot.ErrorDump.exe(session).head.asInstanceOf[Doc]
            message += "\nSnapshot: " + this.errorDump(rawPage, session.spooky)
          }
          if (errorDumpScreenshot) {
            try {
              val rawPage = Screenshot.ErrorScreenshot.exe(session).head.asInstanceOf[Doc]
              message += "\nScreenshot: " + this.errorDump(rawPage, session.spooky)
            } catch {
              case e: Exception =>
                LoggerFactory.getLogger(this.getClass).error("Cannot take screenshot on ActionError:", e)
            }
          }
        } else {
          docOpt.foreach { doc =>
            if (errorDump) {
              message += "\nSnapshot: " + this.errorDump(doc, session.spooky)
            }
          }
        }
    }
    message
  }
}
