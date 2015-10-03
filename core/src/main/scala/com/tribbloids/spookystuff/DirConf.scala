package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.utils.Utils

/**
 * Created by peng on 2/2/15.
 */
class DirConf(
               var root: String = null,//ystem.getProperty("spooky.dirs.root"),
               var localRoot: String = null,
               var _autoSave: String = null,//System.getProperty("spooky.dirs.autosave"),
               var _cache: String = null,//System.getProperty("spooky.dirs.cache"),
               var _errorDump: String = null,//System.getProperty("spooky.dirs.errordump"),
               var _errorScreenshot: String = null,//System.getProperty("spooky.dirs.errorscreenshot"),
               var _checkpoint: String = null,//System.getProperty("spooky.dirs.checkpoint"),
               var _errorDumpLocal: String = null,//System.getProperty("spooky.dirs.errordump.local"),
               var _errorScreenshotLocal: String = null//System.getProperty("spooky.dirs.errorscreenshot.local")
               ) extends Serializable {

  def root_/(subdir: String): String = Option(root).map(Utils.uriSlash(_) + subdir).orNull
  def localRoot_/(subdir: String) = Option(root).map(Utils.uriSlash(_) + subdir).orNull

  def autoSave_=(v: String): Unit = _autoSave = v
  def cache_=(v: String): Unit = _cache = v
  def errorDump_=(v: String): Unit = _errorDump = v
  def errorScreenshot_=(v: String): Unit = _errorScreenshot = v
  def checkpoint_=(v: String): Unit = _checkpoint = v
  def errorDumpLocal_=(v: String): Unit = _errorDumpLocal = v
  def errorScreenshotLocal_=(v: String): Unit = _errorScreenshotLocal = v

  def autoSave: String = Option(_autoSave).getOrElse(root_/("autosave"))
  def cache: String = Option(_autoSave).getOrElse(root_/("cache"))
  def errorDump: String = Option(_autoSave).getOrElse(root_/("errorDump"))
  def errorScreenshot: String = Option(_autoSave).getOrElse(root_/("errorScreenshot"))
  def checkpoint: String = Option(_autoSave).getOrElse(root_/("checkpoint"))
  def errorDumpLocal: String = Option(_autoSave).getOrElse(localRoot_/("errorDump"))
  def errorScreenshotLocal: String = Option(_autoSave).getOrElse(localRoot_/("errorScreenshot"))

  def toJSON: String = {

    Utils.toJson(this, beautiful = true)
  }
}
