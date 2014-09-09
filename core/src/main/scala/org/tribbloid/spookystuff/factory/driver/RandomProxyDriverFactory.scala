package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.phantomjs.PhantomJSDriverService

/**
 * Created by peng on 9/9/14.
 */
class RandomProxyDriverFactory(phantomJSPath: String)
  extends NaiveDriverFactory(phantomJSPath) {

  baseCaps.setCapability(
    PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
    Array("--proxy=127.0.0.1:9050", "--proxy-type=socks5")
  )
}

object RandomProxyDriverFactory {

  def apply(phantomJSPath: String) = new RandomProxyDriverFactory(phantomJSPath)

  def apply() = new RandomProxyDriverFactory(System.getenv("PHANTOMJS_PATH"))
}