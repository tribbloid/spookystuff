package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.metrics.AbstractMetrics
import com.tribbloids.spookystuff.utils.ConfUtils
import org.apache.spark.SparkConf

object Dir extends PluginSystem {

  case class Conf(
      var root: String = null, // System.getProperty("spooky.dirs.root"),
      var localRoot: String = null,
      var autoSave: String = null, // System.getProperty("spooky.dirs.autosave"),
      var cache: String = null, // System.getProperty("spooky.dirs.cache"),
      var errorDump: String = null, // System.getProperty("spooky.dirs.errordump"),
      var errorScreenshot: String = null, // System.getProperty("spooky.dirs.errorscreenshot"),
      var checkpoint: String = null, // System.getProperty("spooky.dirs.checkpoint"),
      var errorDumpLocal: String = null, // System.getProperty("spooky.dirs.errordump.local"),
      var errorScreenshotLocal: String = null // System.getProperty("spooky.dirs.errorscreenshot.local")
  ) extends MutableConfLike {

    import com.tribbloids.spookystuff.utils.SpookyViews._

    override def importFrom(sparkConf: SparkConf): Conf = {

      implicit val _sparkConf: SparkConf = sparkConf

      val _root = Option(root).getOrElse(ConfUtils.getOrDefault("spooky.dirs.root", defaultConf.root))
      val _localRoot =
        Option(localRoot).getOrElse(ConfUtils.getOrDefault("spooky.dirs.root", defaultConf.localRoot))

      val result = Conf(
        root = _root,
        localRoot = _localRoot,
        autoSave = Option(autoSave).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.autosave", _root \\ "autosave")
        ),
        cache = Option(cache).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.cache", _root \\ "cache")
        ),
        errorDump = Option(errorDump).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.dump", _root \\ "errorDump")
        ),
        errorScreenshot = Option(errorScreenshot).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.screenshot", _root \\ "errorScreenshot")
        ),
        checkpoint = Option(checkpoint).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.checkpoint", _root \\ "checkpoint")
        ),
        errorDumpLocal = Option(errorDumpLocal).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.dump.local", _root \\ "errorDumpLocal")
        ),
        errorScreenshotLocal = Option(errorScreenshotLocal).getOrElse(
          ConfUtils.getOrDefault("spooky.dirs.error.screenshot.local", _root \\ "errorScreenshotLocal")
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
