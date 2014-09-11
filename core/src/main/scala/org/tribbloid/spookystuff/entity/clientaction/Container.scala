package org.tribbloid.spookystuff.entity.clientaction

import org.openqa.selenium.By.ByCssSelector
import org.openqa.selenium.{By, WebElement}
import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.factory.PageBuilder

import scala.collection.mutable.ArrayBuffer

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
trait Container extends ClientAction {
  //  override val timeout: Int = Int.MaxValue

  def mayExport: Boolean
}


/**
 * Created by peng on 9/10/14.
 */
//syntax sugar for loop-click-wait
case class LoadMore(
                     selector: String,
                     limit: Int = Const.fetchLimit,
                     intervalMin: Int = Const.actionDelayMin,
                     intervalMax: Int = Const.actionDelayMax,
                     snapshot: Boolean = false,
                     mustExist: String = null
                     ) extends Container {

  assert(limit>0)

  override def mayExport: Boolean = snapshot

  override def doExe(pb: PageBuilder): Array[Page] = {

    val results = new ArrayBuffer[Page]()

    val snapshotAction = Snapshot()
    val delayAction = Delay(intervalMin)
    val clickAction = Click(selector,intervalMax-intervalMin)

    try {
      for (i <- 0 until limit) {
        if (snapshot) results ++= snapshotAction.exe(pb)
        if (mustExist!=null && pb.driver.findElements(By.cssSelector(mustExist)).size()==0) return results.toArray
        delayAction.exe(pb)
        clickAction.exe(pb)
      }
    }
    catch {
      case e: Throwable =>
      //Do nothing, loop until conditions are not met
    }

    results.toArray
  }
}


/**
 * Contains several sub-actions that are iterated for multiple times
 * Will iterate until max iteration is reached or execution is impossible (sub-action throws an exception)
 * @param limit max iteration, default to Const.fetchLimit
 * @param actions a list of actions being iterated through
 */
case class Loop(limit: Int = Const.fetchLimit)(val actions: ClientAction*) extends Container {

  assert(limit>0)

  override def doExe(pb: PageBuilder): Array[Page] = {

    val results = new ArrayBuffer[Page]()

    try {
      for (i <- 0 until limit) {
        for (action <- actions) {
          val pages = action.exe(pb)
          if (pages != null) results.++=(pages)
        }
      }
    }
    catch {
      case e: Throwable =>
      //Do nothing, loop until conditions are not met
    }

    results.toArray
  }

  override def mayExport: Boolean = ClientAction.mayExport(actions: _*)

  override def interpolate[T](map: Map[String,T]): this.type = {

    Loop(limit)(actions.map(_.interpolate(map)): _*).asInstanceOf[this.type ]
  }
}
