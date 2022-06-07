package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.conf.PluginSystem.WithDriver
import com.tribbloids.spookystuff.metrics.AbstractMetrics
import com.tribbloids.spookystuff.web.session.CleanWebDriver
import org.apache.spark.SparkConf

object Web extends WithDriver {

  final val DEFAULT_WEBDRIVER_FACTORY = WebDriverFactory.PhantomJS().taskLocal

  /**
    * otherwise driver cannot do screenshot
    */
  final val TEST_WEBDRIVER_FACTORY = WebDriverFactory.PhantomJS(loadImages = true).taskLocal

  case class Conf(
      var webDriverFactory: DriverFactory[CleanWebDriver] = DEFAULT_WEBDRIVER_FACTORY
  ) extends MutableConfLike {

    override def importFrom(sparkConf: SparkConf): Conf = this.copy()
  }
  def defaultConf: Conf = Conf()

  type Driver = CleanWebDriver

  case class Metrics() extends AbstractMetrics

  case class Plugin(
      spooky: SpookyContext,
      effectiveConf: Conf,
      metrics: Metrics = Metrics()
  ) extends PluginLike {

    override def driverFactory: DriverFactory[CleanWebDriver] = getConf.webDriverFactory

    /**
      * only swap out configuration do not replace anything else
      */
    override def withEffectiveConf(conf: Conf): Plugin = copy(spooky, conf)
  }

  override def default(spooky: SpookyContext): Plugin = {
    Plugin(spooky, defaultConf).withEffectiveConf(defaultConf)
  }
}
