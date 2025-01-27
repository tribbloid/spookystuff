package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.commons.ConfUtils
import com.tribbloids.spookystuff.metrics.AbstractMetrics
import org.apache.spark.SparkConf

object Dir extends PluginSystem {

  case class Conf(
      root: String = null, // System.getProperty("spooky.dirs.root"),
      localRoot: String = null,
      auditing: String = null, // System.getProperty("spooky.dirs.auditing"),
      cache: String = null, // System.getProperty("spooky.dirs.cache"),
      errorDump: String = null, // System.getProperty("spooky.dirs.errordump"),
      errorScreenshot: String = null, // System.getProperty("spooky.dirs.errorscreenshot"),
      checkpoint: String = null, // System.getProperty("spooky.dirs.checkpoint"),
      errorDumpLocal: String = null, // System.getProperty("spooky.dirs.errordump.local"),
      errorScreenshotLocal: String = null // System.getProperty("spooky.dirs.errorscreenshot.local")
  ) extends ConfLike {

    override def importFrom(sparkConf: SparkConf): Conf = {

      implicit val _sparkConf: SparkConf = sparkConf

      val _root = {
        val str = Option(root).getOrElse(ConfUtils.getOrDefault("spooky.dirs.root", defaultConf.root))
        PathMagnet.URI(str)
      }

      val _localRoot =
        Option(localRoot).getOrElse(ConfUtils.getOrDefault("spooky.dirs.root", defaultConf.localRoot))

      // TODO: the following boilerplates can be delegated to pureconfig
      val result = Conf(
        root = _root,
        localRoot = _localRoot,
        auditing = Option(auditing).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.auditing", _root :/ "auditing")
        ),
        cache = Option(cache).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.cache", _root :/ "cache")
        ),
        errorDump = Option(errorDump).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.dump", _root :/ "errorDump")
        ),
        errorScreenshot = Option(errorScreenshot).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.screenshot", _root :/ "errorScreenshot")
        ),
        checkpoint = Option(checkpoint).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.checkpoint", _root :/ "checkpoint")
        ),
        errorDumpLocal = Option(errorDumpLocal).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.dump.local", _root :/ "errorDumpLocal")
        ),
        errorScreenshotLocal = Option(errorScreenshotLocal).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.screenshot.local", _root :/ "errorScreenshotLocal")
        )
      ).asInstanceOf[this.type]
      result
    }
  }
  def defaultConf: Conf = Conf(
    root = "temp",
    localRoot = "temp"
  )

  case class Metrics() extends AbstractMetrics

  case class Plugin(
      spooky: SpookyContext,
      @transient effectiveConf: Conf
  ) extends PluginLike {

    override def withEffectiveConf(conf: Conf): Plugin = copy(effectiveConf = conf)

    override def metrics: Metrics = Metrics()
  }

  override def default(spooky: SpookyContext): Plugin =
    Plugin(
      spooky,
      defaultConf
    ).withConf(defaultConf)
}
