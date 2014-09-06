package org.tribbloid.spookystuff.factory

import org.openqa.selenium.phantomjs.PhantomJSDriverService

/**
 * Created by peng on 07/08/14.
 */
object TorDriverFactory extends NaiveDriverFactory {

  baseCaps.setCapability(
    PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_CLI_ARGS,
    Array("--proxy=127.0.0.1:9050", "--proxy-type=socks5"))
}
