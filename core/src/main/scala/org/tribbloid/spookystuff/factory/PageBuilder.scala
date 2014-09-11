package org.tribbloid.spookystuff.factory

import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import org.openqa.selenium.Capabilities
import org.tribbloid.spookystuff.entity.clientaction.{Snapshot, ClientAction, Interactive, Sessionless}
import org.tribbloid.spookystuff.{Const, SpookyContext}
import org.tribbloid.spookystuff.entity._

import scala.collection.mutable.ArrayBuffer

object PageBuilder {

  def resolve(actions: ClientAction*)(implicit spooky: SpookyContext): Array[Page] = {

    if (ClientAction.snapshotNotOmitted(actions: _*)) {
      resolvePlain(actions: _*)(spooky)
    }
    else {
      resolvePlain(actions :+ Snapshot(): _*)(spooky)
    }
  }

  // Major API shrink! resolveFinal will be merged here
  // if a resolve has no potential to output page then a snapshot will be appended at the end
  private def resolvePlain(actions: ClientAction*)(implicit spooky: SpookyContext): Array[Page] = {

    val results = ArrayBuffer[Page]()

    val pb = if (actions.isEmpty||actions.forall(_.isInstanceOf[Sessionless])) {
      new PageBuilder(spooky, true)
    }
    else {
      new PageBuilder(spooky)
    }

    try {
      for (action <- actions) {
        var pages = action.exe(pb)
        if (pages != null) {

          if (spooky.autoSavePage) pages = pages.map(page => page.save(spooky.pagePath(page))(spooky.hConf) )

          results ++= pages
        }
      }

      results.toArray
    }
    finally {
      pb.finalize()
    }
  }
}

class PageBuilder(
                   val spooky: SpookyContext,
                   sessionless: Boolean = false,
                   caps: Capabilities = null
                   ) {

  val driver = if (sessionless) {
    null
  }
  else {
    val driver = spooky.driverFactory.newInstance(caps)
    driver.manage().timeouts()
      .pageLoadTimeout(Const.resourceTimeout,TimeUnit.SECONDS)
      .setScriptTimeout(Const.resourceTimeout,TimeUnit.SECONDS)

    driver
  }

  val start_time: Long = new Date().getTime
  val backtrace: util.List[Interactive] = new util.ArrayList[Interactive]()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: ClientAction): Array[Page] = action.exe(this)

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize() = {
    try{
      if (driver != null) {
        driver.close()
        driver.quit()
      }
    }catch{
      case t: Throwable => throw t;
    }finally{
      super.finalize()
    }
  }
}