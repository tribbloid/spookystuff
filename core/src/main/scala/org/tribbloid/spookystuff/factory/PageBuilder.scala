package org.tribbloid.spookystuff.factory

import java.io.ObjectInputStream
import java.util.Date

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.{Const, SpookyContext, Utils}

import scala.collection.mutable.ArrayBuffer

object PageBuilder {

  def resolve(actions: Seq[Action], dead: Boolean)(implicit spooky: SpookyContext): Seq[Page] = {

    Utils.retry (Const.resolveRetry){
      if (Action.mayExport(actions: _*) || dead) {
        resolvePlain(actions)(spooky)
      }
      else {
        resolvePlain(actions :+ Snapshot())(spooky) //Don't use singleton, otherwise will flush timestamp
      }
    }

  }

  // Major API shrink! resolveFinal will be merged here
  // if a resolve has no potential to output page then a snapshot will be appended at the end
  private def resolvePlain(actions: Seq[Action])(implicit spooky: SpookyContext): Seq[Page] = {

    //    val results = ArrayBuffer[Page]()

    val pb = new PageBuilder(spooky)()

    try {
      pb ++= actions

      pb.pages
      //      for (action <- actions) {
      //        var pages = action.exe(pb)
      //        if (pages != null) {
      //
      //          if (spooky.autoSave) pages = pages.map(page => page.autoSave(spooky) )
      //
      //          results ++= pages
      //        }
      //      }
      //
      //      results.toArray
    }
    finally {
      pb.finalize()
    }
  }

  def load(fullPath: Path)(hConf: Configuration): Array[Byte] = {

    val fs = fullPath.getFileSystem(hConf)

    if (fs.exists(fullPath)) {

      val fis = fs.open(fullPath)

      val content = IOUtils.toByteArray(fis)

      fis.close()

      content
    }
    else null
  }

  def restore(fullPath: Path)(hConf: Configuration): Page = {

    val fs = fullPath.getFileSystem(hConf)

    if (fs.exists(fullPath)) {
      val fis = fs.open(fullPath)

      val objectIS = new ObjectInputStream(fis)

      val page = objectIS.readObject().asInstanceOf[Page]

      objectIS.close()

      page
    }
    else null
  }

  //  class PrefixFilter(val prefix: String) extends PathFilter {
  //
  //    override def accept(path: Path): Boolean = path.getName.startsWith(prefix)
  //  }
  //
  //  def getDirsByPrefix(dirPath: Path, prefix: String)(hConf: Configuration): Seq[Path] = {
  //
  //    val fs = dirPath.getFileSystem(hConf)
  //
  //    if (fs.getFileStatus(dirPath).isDir) {
  //      val status = fs.listStatus(dirPath, new PrefixFilter(prefix))
  //
  //      status.map(_.getPath)
  //    }
  //    else Seq()
  //  }

  //restore latest in a directory
  def restoreLatest(dirPath: Path)(hConf: Configuration): Seq[Page] = {

    val fs = dirPath.getFileSystem(hConf)

    if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDir) {
      val results = new ArrayBuffer[Page]()

      val statuses = fs.listStatus(dirPath)

      val latestStatus = statuses.filter(!_.isDir).sortBy(_.getModificationTime).lastOption

      latestStatus match {
        case Some(status) => results += restore(status.getPath)(hConf)
        case _ =>
      }

      val subDirs = statuses.filter(_.isDir).map(_.getPath).sortBy(_.getName)
      for (path <- subDirs) {
        results ++= restoreLatest(path)(hConf)
      }

      results
    }
    else null
  }
}

class PageBuilder(
                   val spooky: SpookyContext,
                   caps: Capabilities = null,
                   val startTime: Long = new Date().getTime
                   )(
                   val autoSave: Boolean = spooky.autoSave,
                   val autoCache: Boolean = spooky.autoCache,
                   val autoRestore: Boolean = spooky.autoRestore
                   ) {

  private var _driver: WebDriver = null

  //mimic lazy val but retain ability to destruct it on demand
  def driver: WebDriver = {
    if (_driver == null) _driver = spooky.driverFactory.newInstance(caps)
    _driver
  }

  val backtrace: ArrayBuffer[Action] = ArrayBuffer() //kept in session, but it is action deciding if they fit in or what part to export

  private val buffer: ArrayBuffer[Action] = ArrayBuffer()
  private val pages: ArrayBuffer[Page] = ArrayBuffer()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize() = {
    try{
      if (_driver != null) {
        _driver.close()
        _driver.quit()
      }
    }catch{
      case t: Throwable => throw t;
    }finally{
      super.finalize()
    }
  }

  def restore(action: Action): Seq[Page] = {
    val uid = PageUID(buffer :+ action)

    val path = new Path(
      Utils.urlConcat(
        spooky.autoCacheRoot,
        spooky.PagePathLookup(uid).toString
      )
    )

    PageBuilder.restoreLatest(path)(spooky.hConf)
  }

  //lazy execution by default.
  def +=(action: Action): Unit = {
    if (action.mayExport()) {

      //always try to read from cache first
      val restored = if (autoRestore) restore(action)
      else null

      if (restored != null) pages ++= restored
      else {

        for (buffered <- this.buffer)
        {
          pages ++= buffered.exe(this)() // I know buffered one should only have empty result, just for safety
        }
        buffer.clear()
        var batch = action.exe(this)()

        if (autoSave) batch = batch.map(_.autoSave(spooky))
        if (autoCache) batch = batch.map(_.autoCache(spooky))

        pages ++= batch
      }
    }
    else {
      this.buffer += action
    }

    this.backtrace ++= action.trunk()//always put into backtrace.
  }

  def ++= (actions: Seq[Action]): Unit = {
    actions.foreach(this += _)
  }

}