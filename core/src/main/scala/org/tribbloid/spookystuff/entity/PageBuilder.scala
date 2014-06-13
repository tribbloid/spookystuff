package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.openqa.selenium.phantomjs.PhantomJSDriver
import scala.collection.mutable.ArrayBuffer

import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import java.util
import org.openqa.selenium.remote.RemoteWebDriver
import scala.collection.JavaConversions._

object PageBuilder {

  def resolveFinal(ac: ActionChain): Page = {
    val interactions = ac.actions.collect{
      case i: Interaction => i
    }

    resolveFinal(interactions: _*)( ac.context )
  }

  //shorthand for resolving the final stage after some interactions
  def resolveFinal(actions: Interaction*)(implicit context: util.Map[String,String] = null): Page = {
    var result: Page = null
    val allActions = actions.seq.:+(Snapshot())
    PageBuilder.resolve(allActions: _*)(context).foreach{
      page => {
        result = page
        //TODO: break;
      }
    }
    result
  }

  def resolve(ac: ActionChain): Seq[Page] = this.resolve(ac.actions: _*)(ac.context)

  def resolve(actions: Action*)(implicit context: util.Map[String,String] = null): Seq[Page] = {

    val results = ArrayBuffer[Page]()

    val builder = new PageBuilder(context)

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

//TODO: need refactoring to accept more openqa drivers
private class PageBuilder(
                           val context: util.Map[String,String] = null,
                           val driver: RemoteWebDriver = new PhantomJSDriver(Conf.phantomJSCaps)
                           ) {

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

  def exe(action: Extraction): Page = {
    val page = action.exe(this.driver)
    page.backtrace.addAll(this.backtrace)
    page.context = this.context
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