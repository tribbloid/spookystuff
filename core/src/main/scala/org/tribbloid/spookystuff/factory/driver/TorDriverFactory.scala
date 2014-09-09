package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.phantomjs.PhantomJSDriverService

/**
 * Created by peng on 07/08/14.
 */
class TorDriverFactory(phantomJSPath: String)
  extends NaiveDriverFactory(phantomJSPath: String) {

  baseCaps.setCapability(
    PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
    Array("--proxy=127.0.0.1:9050", "--proxy-type=socks5")
  )
}

object TorDriverFactory {

  def apply(phantomJSPath: String) = new TorDriverFactory(phantomJSPath)

  def apply() = new TorDriverFactory(System.getenv("PHANTOMJS_PATH"))
}