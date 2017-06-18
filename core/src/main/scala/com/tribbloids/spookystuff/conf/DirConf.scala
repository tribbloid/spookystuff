package com.tribbloids.spookystuff.conf

import org.apache.spark.SparkConf

object DirConf extends Submodules.Builder[DirConf]{

  def default = DirConf(
    root = "temp",
    localRoot = "temp"
  )
}

/**
  * Created by peng on 2/2/15.
  */
case class DirConf(
                    var root: String = null, //System.getProperty("spooky.dirs.root"),
                    var localRoot: String = null,
                    var autoSave: String = null, //System.getProperty("spooky.dirs.autosave"),
                    var cache: String = null, //System.getProperty("spooky.dirs.cache"),
                    var errorDump: String = null, //System.getProperty("spooky.dirs.errordump"),
                    var errorScreenshot: String = null, //System.getProperty("spooky.dirs.errorscreenshot"),
                    var checkpoint: String = null, //System.getProperty("spooky.dirs.checkpoint"),
                    var errorDumpLocal: String = null, //System.getProperty("spooky.dirs.errordump.local"),
                    var errorScreenshotLocal: String = null //System.getProperty("spooky.dirs.errorscreenshot.local")
                  ) extends AbstractConf {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  // TODO: use reflection to automate and move to AbstractConf
  override def importFrom(sparkConf: SparkConf): this.type = {

    val _root = Option(root).getOrElse(AbstractConf.getOrDefault("spooky.dirs.root", DirConf.default.root))
    val _localRoot = Option(localRoot).getOrElse(AbstractConf.getOrDefault("spooky.dirs.root", DirConf.default.localRoot))

    implicit val conf = sparkConf

    val result = new DirConf(
      root = _root,
      localRoot = _localRoot,
      autoSave = Option(autoSave).getOrElse(AbstractConf.getOrDefault("spooky.dirs.autosave", _root \\ "autosave")),
      cache = Option(cache).getOrElse(AbstractConf.getOrDefault("spooky.dirs.cache", _root \\ "cache")),
      errorDump = Option(errorDump).getOrElse(AbstractConf.getOrDefault("spooky.dirs.error.dump", _root \\ "errorDump")),
      errorScreenshot = Option(errorScreenshot).getOrElse(AbstractConf.getOrDefault("spooky.dirs.error.screenshot", _root \\ "errorScreenshot")),
      checkpoint = Option(checkpoint).getOrElse(AbstractConf.getOrDefault("spooky.dirs.checkpoint", _root \\ "checkpoint")),
      errorDumpLocal = Option(errorDumpLocal).getOrElse(AbstractConf.getOrDefault("spooky.dirs.error.dump.local", _root \\ "errorDumpLocal")),
      errorScreenshotLocal = Option(errorScreenshotLocal).getOrElse(AbstractConf.getOrDefault("spooky.dirs.error.screenshot.local", _root \\ "errorScreenshotLocal"))
    )
      .asInstanceOf[this.type]
    result
  }
}