package org.tribbloid.spookystuff.entity.client

import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.{Page, PageUID}
import org.tribbloid.spookystuff.factory.PageBuilder

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
abstract class Block extends Action

case class Try(actions: Seq[Action]) extends Block {

  override def mayExport(): Boolean = Action.mayExport(actions: _*)

  final override def trunk() = {
    val trunked = actions.flatMap {
      case interaction: Interaction => Some(interaction)
      case export: Export => None
      case container: Block => container.trunk()
    }

    Some(Try(trunked))
  }

  override def doExe(pb: PageBuilder): Seq[Page] = {

    val pages = new ArrayBuffer[Page]()

    try {
      for (action <- actions) {
        pages ++= action.exe(pb)(errorDump = false)
      }
    }
    catch {
      case e: Throwable =>
      //Do nothing because just trying
    }

    pages.zipWithIndex.map(tuple => tuple._1.copy(uid = PageUID(pb.backtrace :+ this,tuple._2)))
  }

  override def interpolateFromMap[T](map: Map[String,T]): this.type = {

    this.copy(actions = actions.map(_.interpolateFromMap(map))).asInstanceOf[this.type]
  }
}

/**
 * Contains several sub-actions that are iterated for multiple times
 * Will iterate until max iteration is reached or execution is impossible (sub-action throws an exception)
 * @param limit max iteration, default to Const.fetchLimit
 * @param actions a list of actions being iterated through
 */
case class Loop(
                 actions: Seq[Action],
                 limit: Int = Const.fetchLimit
                 ) extends Block {

  assert(limit>0)

  override def mayExport(): Boolean = Action.mayExport(actions: _*)

  final override def trunk() = {
    val trunked = actions.flatMap {
      case interaction: Interaction => Some(interaction)
      case export: Export => None
      case container: Block => container.trunk()
    }

    Some(Loop(trunked, limit))
  }

  override def doExe(pb: PageBuilder): Seq[Page] = {

    val pages = new ArrayBuffer[Page]()

    try {
      for (i <- 0 until limit) {

        for (action <- actions) {
          pages ++= action.exe(pb)(errorDump = false)
        }
      }
    }
    catch {
      case e: Throwable =>
      //Do nothing, loop until not possible
    }

    pages.zipWithIndex.map(tuple => tuple._1.copy(uid = PageUID(pb.backtrace :+ this,tuple._2)))
  }

  override def interpolateFromMap[T](map: Map[String,T]): this.type = {

    this.copy(actions = actions.map(_.interpolateFromMap(map))).asInstanceOf[this.type]
  }
}

/**
 * Created by peng on 9/10/14.
 */
//syntax sugar for loop-click-wait
case class LoadMore(
                     selector: String,
                     limit: Int = Const.fetchLimit,
                     intervalMin: Duration = Const.actionDelayMin,
                     intervalMax: Duration = Const.actionDelayMax,
                     snapshot: Boolean = false
                     ) extends Block {

  assert(limit>0)

  override def mayExport(): Boolean = snapshot

  override def doExe(pb: PageBuilder): Seq[Page] = {

    val pages = new ArrayBuffer[Page]()

    val snapshotAction = Snapshot()
    val delayAction = Delay(intervalMin)
    val clickAction = Click(selector).in(intervalMax-intervalMin)

    try {
      for (i <- 0 until limit) {

        if (snapshot) pages ++= snapshotAction.exe(pb)(errorDump = false)
        delayAction.exe(pb)(errorDump = false)
        clickAction.exe(pb)(errorDump = false)
      }
    }
    catch {
      case e: Throwable =>
      //Do nothing, loop until conditions are not met
    }

    pages.zipWithIndex.map(tuple => tuple._1.copy(uid = PageUID(pb.backtrace :+ this,tuple._2)))
  }

  //the minimal equivalent action that can be put into backtrace
  override def trunk(): Option[Action] = Some(this.copy(snapshot = false))
}