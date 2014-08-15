package org.tribbloid.spookystuff.factory

import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.openqa.selenium.Capabilities
import org.openqa.selenium.remote.server.DriverFactory
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._

import scala.collection.mutable.ArrayBuffer

object PageBuilder {

  //shorthand for resolving the final stage after some interactions
  lazy val emptyPage: Page = {
    val pb = new PageBuilder(null)

    try {
      Snapshot().exe(pb).toList(0)
    }
    finally {
      pb.finalize
    }
  }

  def resolve(actions: Action*)(hConf: Configuration): Array[Page] = {
    if (ActionUtils.mayHaveResult(actions: _*) == true) {
      resolvePlain(actions: _*)(hConf)
    }
    else
    {
      resolvePlain(actions.:+(Snapshot()): _*)(hConf)
    }
  }

  // Major API shrink! resolveFinal will be merged here
  // if a resolve has no potential to output page then a snapshot will be appended at the end
  private def resolvePlain(actions: Action*)(hConf: Configuration): Array[Page] = {

    val results = ArrayBuffer[Page]()

    val pb = if (actions.forall( _.isInstanceOf[Sessionless] )) {
      new PageBuilder(hConf, null)
    }
    else {
      new PageBuilder(hConf)
    }

    try {
      for (action <- actions) {
        val pages = action.exe(pb)
        if (pages != null) results.++=(pages)
      }

      return results.toArray
    }
    finally {
      pb.finalize
    }
  }

}

class PageBuilder(
                   val hConf: Configuration,
                   val driverFactory: DriverFactory = Conf.defaultDriverFactory,
                   val caps: Capabilities = null
                   ) {

  val driver = if (driverFactory != null) {
    this.driverFactory.newInstance(caps)
  }
  else {
    null
  }

  val start_time: Long = new Date().getTime
  val backtrace: util.List[Interactive] = new util.ArrayList[Interactive]()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    try{
      if (driver != null) {
        driver.close()
        driver.quit()
      }
    }catch{
      case t: Throwable => throw t;
    }finally{
      super.finalize();
    }
  }
}