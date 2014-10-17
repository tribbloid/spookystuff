package org.tribbloid.spookystuff.entity.client

import org.tribbloid.spookystuff.{Utils, Const}
import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.factory.PageBuilder

import scala.concurrent.duration.Duration

/**
 * Created by peng on 04/06/14.
 */

object Action {

  def interpolateFromMap[T](str: String, map: collection.Map[String,T]): String = {
    if ((str == null)|| str.isEmpty) return str
    else if ((map == null)|| map.isEmpty) return str
    var strVar = str
    for (entry <- map) {
      val sub = "#{".concat(entry._1).concat("}")
      if (strVar.contains(sub))
      {
        var value: String = "null"
        if (entry._2 != null) {value = entry._2.toString}
        strVar = strVar.replace(sub, value)
      }
    }
    strVar
  }

  def mayExport(actions: Action*): Boolean = {
    for (action <- actions) {
      if (action.mayExport()) return true
    }
    false
  }

  //  def snapshotNotOmitted(actions: Action*): Boolean = {
  //    if (actions.isEmpty) {
  //      true //indicating a dead action chain
  //    }
  //    else {
  //      mayExport(actions: _*)
  //    }
  //  }
}

/**
 * These are the same actions a human would do to get to the data page,
 * their order of execution is identical to that they are defined.
 * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
 * by inserting keys enclosed by `#{}`, in execution they will be replaced with values they map to.
 * This is used almost exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere.
 * extends Product to make sure all subclasses are case classes
 */
trait Action extends Serializable with Product {

  private var timeElapsed: Long = -1 //only set once

  //  val optional: Boolean

  def interpolateFromMap[T](map: Map[String,T]): this.type = this

  protected var delay: Duration = Const.actionDelayMax

  //this should handle autoSave, cache and errorDump
  def exe(
           session: PageBuilder
           )(
           errorDump: Boolean = session.spooky.errorDump,
           errorDumpScreenshot: Boolean = session.spooky.errorDumpScreenshot
           ): Seq[Page] = {

    val results = try {
      Utils.withDeadline(delay + session.spooky.resourceTimeout) {
        doExe(session)
      }
    }
    catch {
      case e: Throwable =>

        var isInBacktrace = false

        var message: String = "\n[backtrace]"

        message += session.backtrace.map{
          action =>{
            if (action eq this) {
              isInBacktrace = true
              "+>"+action.toString
            }
            else "| "+action.toString
          }
        }.mkString("\n")+"\n"

        if (!isInBacktrace) message += "+>" + this

        if (!this.isInstanceOf[Sessionless]) {
          if (errorDump) {
            var page = DefaultSnapshot.doExe(session).toList(0)
            try {
              page = page.errorDump(session.spooky)
              message += "\n"+"snapshot saved to: " + page.saved
            }
            catch {
              case e: Throwable =>
                try {
                  page = page.localErrorDump(session.spooky)
                  message += "\n"+"distributed file system inaccessible.........snapshot saved to: " + page.saved
                }
                catch {
                  case e: Throwable =>
                    message += "\n"+"all file systems inaccessible.........snapshot not saved"
                }
            }
          }
          if (errorDumpScreenshot) {
            var page = DefaultScreenshot.doExe(session).toList(0)
            try {
              page = page.errorDump(session.spooky)
              message += "\n"+"screenshot saved to: " + page.saved
            }
            catch {
              case e: Throwable =>
                try {
                  page = page.localErrorDump(session.spooky)
                  message += "\n"+"distributed file system inaccessible.........screenshot saved to: " + page.saved
                }
                catch {
                  case e: Throwable =>
                    message += "\n"+"all file systems inaccessible.........screenshot not saved"
                }
            }
          }
        }

        val ex = new ActionException(message, e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }

    this.timeElapsed = System.currentTimeMillis() - session.startTime

    results
  }

  def doExe(session: PageBuilder): Seq[Page]

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  def mayExport(): Boolean

  //the minimal equivalent action that can be put into backtrace
  def trunk(): Option[Action]
}

trait Timed extends Action{

  def in(delay: Duration): this.type = {
    this.delay = delay
    this
  }
}

trait Sessionless extends Action