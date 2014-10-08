package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.remote.DesiredCapabilities
import org.openqa.selenium.{WebDriver, Capabilities}
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.tribbloid.spookystuff.{Const, Utils}

import scala.util.Random

/**
 * Created by peng on 9/9/14.
 */

class RandomProxyDriverFactory(
                                phantomJSPath: String,
                                val proxies: (String, String)*
                                )
  extends NaiveDriverFactory(phantomJSPath) {

  override def newInstance(capabilities: Capabilities): WebDriver = {
    val proxyCap = new DesiredCapabilities(baseCaps).merge(capabilities)
    val proxy = proxies(Random.nextInt(proxies.size))

    proxyCap.setCapability(
      PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
      Array("--proxy=" + proxy._1, "--proxy-type=" + proxy._2)
    )

    Utils.retry () {
      Utils.withDeadline(Const.sessionInitializationTimeout) {
        new PhantomJSDriver(proxyCap)
      }
    }
  }
}

object RandomProxyDriverFactory {

  def apply(
             phantomJSPath: String,
             proxies: (String, String)*
             ) = new RandomProxyDriverFactory(phantomJSPath, proxies: _*)

  def apply(proxies: (String, String)*) = new RandomProxyDriverFactory(System.getenv("PHANTOMJS_PATH"), proxies: _*)
}