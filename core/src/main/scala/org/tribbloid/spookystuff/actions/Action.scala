package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.pages.PageLike
import org.tribbloid.spookystuff.session.Session
import org.tribbloid.spookystuff.utils.Utils
import org.tribbloid.spookystuff.{ActionException, Const}

import scala.concurrent.duration.Duration

/**
 * Created by peng on 04/06/14.
 */

/**
 * These are the same actions a human would do to get to the data page,
 * their order of execution is identical to that they are defined.
 * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
 * by inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to.
 * This is used almost exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere.
 * extends Product to make sure all subclasses are case classes
 */
trait Action extends ActionLike {

  private var timeElapsed: Long = -1 //only set once

  //  val optional: Boolean

  //this should handle autoSave, cache and errorDump
  def apply(session: Session): Seq[PageLike] = {

    val errorDump: Boolean = session.spooky.conf.errorDump
    val errorDumpScreenshot: Boolean = session.spooky.conf.errorScreenshot

    val results = try {
      this match { //temporarily disabled as we assume that DFS is the culprit for causing deadlock
        case tt: Timed =>

          Utils.withDeadline(tt.hardTerminateTimeout(session)) {
            doExe(session)
          }
        case _ =>
          doExe(session)
      }
    }
    catch {
      case e: Throwable =>

        var message: String = "\n"

        message += session.backtrace.map{
          action =>
            "| "+action.toString
        }.mkString("\n")

        message += "\n+>" + this.toString

        if (!this.isInstanceOf[Driverless] ) {
          if (errorDump) {
            val rawPage = DefaultSnapshot.doExe(session).toList(0)
            val uid = rawPage.uid.copy(backtrace = Trace(rawPage.uid.backtrace.self :+ this))
            val page = rawPage.copy(uid = uid)
            try {
              page.errorDump(session.spooky)
              message += "\n"+"snapshot saved to: " + page.saved
            }
            catch {
              case e: Throwable =>
                try {
                  page.errorDumpLocal(session.spooky)
                  message += "\n"+"distributed file system inaccessible.........snapshot saved to: " + page.saved
                }
                catch {
                  case e: Throwable =>
                    message += "\n"+"all file systems inaccessible.........snapshot not saved"
                }
            }
          }
          if (errorDumpScreenshot) {
            val rawPage = DefaultScreenshot.doExe(session).toList(0)
            val uid = rawPage.uid.copy(backtrace = Trace(rawPage.uid.backtrace.self :+ this))
            val page = rawPage.copy(uid = uid)
            try {
              page.errorDump(session.spooky)
              message += "\n"+"screenshot saved to: " + page.saved
            }
            catch {
              case e: Throwable =>
                try {
                  page.errorDumpLocal(session.spooky)
                  message += "\n"+"distributed file system inaccessible.........screenshot saved to: " + page.saved
                }
                catch {
                  case e: Throwable =>
                    message += "\n"+"all file systems inaccessible.........screenshot not saved"
                }
            }
          }
        }
        else{
          message += "\n"+"(no snapshot or screenshot for driverless action)"
        }

        val ex = new ActionException(message, e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }

    this.timeElapsed = System.currentTimeMillis() - session.startTime

    results
  }

  def doExe(session: Session): Seq[PageLike]

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.timeElapsed = same.asInstanceOf[Action].timeElapsed
  }
}

trait Timed extends Action {

  private var _timeout: Duration = null

  def in(deadline: Duration): this.type = {
    this._timeout = deadline
    this
  }

  def timeout(session: Session): Duration = {
    val base = if (this._timeout == null) session.spooky.conf.remoteResourceTimeout
    else this._timeout

    base
  }

  def hardTerminateTimeout(session: Session): Duration = {
    timeout(session) + Const.hardTerminateOverhead
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}

trait Named extends Action {

  var name: String = this.toString

  def as(name: Symbol): this.type = {
    assert(name != null)

    this.name = name.name
    this
  }

  final def ~(name: Symbol): this.type = as(name)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.name = same.asInstanceOf[Named].name
  }
}

trait Driverless extends Action