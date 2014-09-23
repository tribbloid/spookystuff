package org.tribbloid.spookystuff.entity.client

import org.tribbloid.spookystuff.Const
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

  //this should handle autoSave, cache and errorDump
  def exe(
           session: PageBuilder
           )(
           errorDump: Boolean = session.spooky.errorDump
           ): Seq[Page] = {

    val results = try {
      doExe(session) //TODO: if mayExport, read from cache first
    }
    catch {
      case e: Throwable =>

        var isInBacktrace = false

        var message: String = "[[[backtrace]]]\n"

        message += session.backtrace.map{
          action =>{
            if (action eq this) {
              isInBacktrace = true
              "+>"+action.toString
            }
            else "| "+action.toString
          }
        }.mkString("\n")+"\n"

        if (!isInBacktrace) message += "|>" + this+"\n"

        if ((!this.isInstanceOf[Sessionless]) && errorDump) {
          var page = DefaultSnapshot.exe(session)(errorDump = false).toList(0)
          try {
            page = page.errorDump(session.spooky)
            message += "snapshot saved to: " + page.saved
          }
          catch {
            case e: Throwable =>
              try {
                page = page.localErrorDump(session.spooky)
                message += "distributed file system inaccessible.........snapshot saved to: " + page.saved
              }
              catch {
                case e: Throwable =>
                  message += "all file systems inaccessible.........snapshot not saved"
              }
          }
        }
        throw new ActionException(message, e)
    }

    if (this.mayExport()) {

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
  var delay: Duration = Const.actionDelayMax

  def in(delay: Duration): this.type = {
    this.delay = delay
    this
  }
}

trait Sessionless extends Action