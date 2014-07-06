package org.tribbloid.spookystuff.entity

import java.util.Date

import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.tribbloid.spookystuff.Conf
import scala.collection.mutable.ArrayBuffer

import java.util
import org.openqa.selenium.remote.RemoteWebDriver

object PageBuilder {

  //shorthand for resolving the final stage after some interactions
  lazy val emptyPage: Page = {
    val pb = new PageBuilder()

    try {
      Snapshot().exe(pb).toList(0)
    }
    finally {
      pb.finalize
    }
  }

  def resolveFinal(actions: Action*): Page = {

    val Interactions = actions.collect{
      case i: Interactive => i
      case i: Container => i
    }

    if (Interactions.length == 0) return emptyPage

    val pb = new PageBuilder()
    try {
      for (action <- Interactions) {
        action.exe(pb)
      }
      return Snapshot().exe(pb).toList(0)
    }
    finally {
      pb.finalize
    }
  }

  def resolve(actions: Action*): Array[Page] = {

    val results = ArrayBuffer[Page]()
    if (actions.forall( _.isInstanceOf[Sessionless] )) {
      actions.foreach {
        action => results.++=(action.exe(new PageBuilder(null)))
      }

      return results.toArray
    }
    else {
      val pb = new PageBuilder()

      try {
        actions.foreach {
          action => {
            val pages = action.exe(pb)
            if (pages != null) results.++=(pages)
          }
        }

        return results.toArray
      }
      finally {
        pb.finalize
      }
    }
  }

}

//TODO: avoid passing a singleton driver!
private class PageBuilder(
                           val driver: RemoteWebDriver = new PhantomJSDriver(Conf.phantomJSCaps)
                           ) {

  val start_time: Long = new Date().getTime
  val backtrace: util.List[Interactive] = new util.ArrayList[Interactive]()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    try{
      driver.close()
      driver.quit()
    }catch{
      case t: Throwable => throw t;
    }finally{
      super.finalize();
    }
  }
}