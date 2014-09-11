package org.tribbloid.spookystuff.entity.clientaction

import java.util.Date

import org.tribbloid.spookystuff.Utils
import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 04/06/14.
 */

object ClientAction {

  def interpolate[T](str: String, map: collection.Map[String,T]): String = {
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

  def mayExport(actions: ClientAction*): Boolean = {
    for (action <- actions) {
      action match {
        case a: Export => return true
        case a: Container =>
          if (a.mayExport) return true
        case a: Interactive =>
        case _ =>
      }
    }
    false
  }

  def snapshotNotOmitted(actions: ClientAction*): Boolean = {
    if (actions.isEmpty) {
      true //indicating a dead action chain
    }
    else {
      mayExport(actions: _*)
    }
  }
}

/**
 * These are the same actions a human would do to get to the data page,
 * their order of execution is identical to that they are defined.
 * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
 * by inserting keys enclosed by `#{}`, in execution they will be replaced with values they map to.
 * This is used almost exclusively in typing into a textbox, but it's flexible enough to be used anywhere.
 */
trait ClientAction extends Serializable with Cloneable {

  var timeline: Long = -1 //this should be a val

  override def clone(): AnyRef = super.clone()

  private var canFail: Boolean = false //TODO: change to trait?

  def canFail(value: Boolean = true): this.type = {
    this.canFail = value
    this
  }

  def interpolate[T](map: Map[String,T]): this.type = this

  final def exe(pb: PageBuilder): Array[Page] = {

    try {
      val pages = this match {
        case d: Timed =>
          Utils.withDeadline(d.timeout) {
            doExe(pb: PageBuilder)
          }
        case _ => doExe(pb: PageBuilder)
      }

      val newTimeline = new Date().getTime - pb.start_time

      if (this.isInstanceOf[Interactive]) {
        val cloned = this.clone().asInstanceOf[Interactive] //TODO: EVIL! CHANGE TO copy if possible
        cloned.timeline = newTimeline
        pb.backtrace.add(cloned)
      }

      //      if (this.isInstanceOf[Export]) {
      //        pages = pages.map(page => page.copy(alias = this.asInstanceOf[Export].alias))
      //      }

      pages
    }
    catch {
      case e: Throwable =>

        if (this.isInstanceOf[Interactive]) {

          val page = Snapshot().exe(pb).toList(0)
          try {
            page.save(pb.spooky.errorDumpPath(page))(pb.spooky.hConf)
          }
          catch {
            case e: Throwable =>
              page.saveLocal(pb.spooky.localErrorDumpPath(page))
          }
          // TODO: logError("error Page saved as "+errorFileName)
        }

        if (!canFail) {
          throw e
        }
        else {
          null
        }
    }
  }

  def doExe(pb: PageBuilder): Array[Page]
}