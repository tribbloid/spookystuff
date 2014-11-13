package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.phantomjs.PhantomJSDriverService

/**
 * Created by peng on 07/08/14.
 */
class TorDriverFactory(
                        phantomJSPath: String,
                        loadImages: Boolean,
                        resolution: (Int,Int)
                        )
  extends NaiveDriverFactory(
    phantomJSPath: String,
    loadImages: Boolean,
    resolution: (Int,Int)
  ) {

  baseCaps.setCapability(
    PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
    Array("--proxy=127.0.0.1:9050", "--proxy-type=socks5")
  )
}

object TorDriverFactory {

  def apply(
             phantomJSPath: String = System.getenv("PHANTOMJS_PATH"),
             loadImages: Boolean = false,
             userAgent: String = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
             resolution: (Int,Int) = (1920, 1080)
             ) = new TorDriverFactory(phantomJSPath, loadImages, resolution)
}