package org.tribbloid.spookystuff.entity

import java.util.Date

import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.tribbloid.spookystuff.Conf
import scala.collection.mutable.ArrayBuffer

import java.util
import org.openqa.selenium.remote.RemoteWebDriver

object PageBuilder {

  //shorthand for resolving the final stage after some interactions
  def resolveFinal(interactions: Interaction*): HtmlPage = {
    var result: HtmlPage = null
    val allActions = interactions.seq.:+(Snapshot())
    PageBuilder.resolve(allActions: _*).foreach{
      page => {
        result = page
        //TODO: break;
      }
    }
    result
  }

  def resolve(actions: Action*): Seq[HtmlPage] = {

    val results = ArrayBuffer[HtmlPage]()

    val builder = new PageBuilder()

    try {
      actions.foreach {
        action => action match {
          case a: Interaction => {
            builder.exe(a)
          }
          case a: Extraction => {
            results += builder.exe(a)
          }
          case a: Dump => {
            builder.exe(a)
          }
          case _ => throw new UnsupportedOperationException
        }
      }
    }
    finally
    {
      builder.finalize
    }

    return results.toSeq
  }

}

//TODO: avoid passing a singleton driver!
private class PageBuilder(val driver: RemoteWebDriver = new PhantomJSDriver(Conf.phantomJSCaps)) {

  val start_time: Long = new Date().getTime
  val backtrace: util.List[Interaction] = new util.ArrayList[Interaction]()
  //by default drivers should be reset and reused in this case, but whatever

  def exe(action: Interaction) = {
    try {
      action.exe(this.driver)
      action.timeline = new Date().getTime - start_time
      backtrace.add(action)
    }
    catch {
      case e: Throwable => {
        val page = Snapshot().exe(this.driver)
        val filename = page.hashCode().toString+".error"
        page.save(filename)
        //        TODO: logError("Error Page saved as "+filename)
        throw e //try to delegate all failover to Spark, but this may change in the future
      }
    }
  }

  def exe(action: Extraction): HtmlPage = {
    val page = action.exe(this.driver)
    page.backtrace.addAll(this.backtrace)
    return page
  }

  def exe(action: Dump) {
    action.exe(this.driver)
  }

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    driver.quit()
  }
}