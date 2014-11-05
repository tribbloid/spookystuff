package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.Capabilities
import org.openqa.selenium.phantomjs.PhantomJSDriverService
import org.tribbloid.spookystuff.SpookyContext

import scala.util.Random

/**
 * Created by peng on 9/9/14.
 */

class RandomProxyDriverFactory(
                                proxies: Seq[(String, String)],
                                phantomJSPath: String,
                                loadImages: Boolean,
                                userAgent: String,
                                resolution: (Int,Int)
                                )
  extends NaiveDriverFactory(
    phantomJSPath,
    loadImages,
    userAgent,
    resolution
  ) {

  override def newCap(capabilities: Capabilities, spooky: SpookyContext) = {
    val cap = super.newCap(capabilities,spooky)

    val proxy = proxies(Random.nextInt(proxies.size))
    cap.setCapability(
      PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
      Array("--proxy=" + proxy._1, "--proxy-type=" + proxy._2)
    )

    cap
  }

}

object RandomProxyDriverFactory {

  def apply(
             proxies: Seq[(String, String)],
             phantomJSPath: String = System.getenv("PHANTOMJS_PATH"),
             loadImages: Boolean = false,
             userAgent: String = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
             resolution: (Int,Int) = (1920, 1080)
             ): RandomProxyDriverFactory = new RandomProxyDriverFactory(proxies, phantomJSPath, loadImages, userAgent, resolution)
}