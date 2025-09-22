package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.metrics.SpookyMetrics

object Core extends PluginSystem {

  type Conf = SpookyConf

  type Metrics = SpookyMetrics

  case class Plugin(
      spooky: SpookyContext,
      @transient effectiveConf: SpookyConf,
      override val metrics: SpookyMetrics = SpookyMetrics()
  ) extends PluginLike {

    override def withEffectiveConf(conf: SpookyConf): Plugin = copy(effectiveConf = conf)
  }

  override def getDefault(spooky: SpookyContext): Plugin = {
    val default = SpookyConf.default
    Plugin(spooky, default).withConf(default)
  }
}
