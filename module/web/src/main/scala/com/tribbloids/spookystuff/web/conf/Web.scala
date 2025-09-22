package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.conf.DriverSystem
import com.tribbloids.spookystuff.metrics.AbstractMetrics
import com.tribbloids.spookystuff.web.agent.CleanWebDriver
import org.apache.spark.SparkConf

object Web extends DriverSystem {

  case class Conf(
      webDriverFactory: WebDriverFactory = WebDriverFactory.default
  ) extends ConfLike {

    override def importFrom(sparkConf: SparkConf): Conf = this.copy()
  }
  def defaultConf: Conf = Conf()

  type Driver = CleanWebDriver

  case class Metrics() extends AbstractMetrics

  case class Plugin(
      spooky: SpookyContext,
      effectiveConf: Conf,
      metrics: Metrics = Metrics()
  ) extends _PluginLike {

    override def driverFactory: DriverFactory[CleanWebDriver] = getConf.webDriverFactory

    /**
      * only swap out configuration do not replace anything else
      */
    override def withEffectiveConf(conf: Conf): Plugin = copy(spooky, conf)

  }

  override def getDefault(spooky: SpookyContext): Plugin = {
    Plugin(spooky, defaultConf).withEffectiveConf(defaultConf)
  }
}
