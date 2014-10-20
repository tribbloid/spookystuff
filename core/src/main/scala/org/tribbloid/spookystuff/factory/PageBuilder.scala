package org.tribbloid.spookystuff.factory

import java.io.ObjectStreamException
import java.util.Date

import org.openqa.selenium.{Capabilities, WebDriver}
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.{Const, SpookyContext, Utils}

import scala.collection.mutable.ArrayBuffer

object PageBuilder {

  def autoSnapshot(actions: Seq[Action], dead: Boolean): Seq[Action] = {
    if (Action.mayExport(actions) || dead) actions
    else actions :+ Snapshot() //Don't use singleton, otherwise will flush timestamp
  }

  def resolve(actions: Seq[Action], dead: Boolean)(implicit spooky: SpookyContext): Seq[Page] = {

    Utils.retry (Const.remoteResourceInPartitionRetry){
        resolvePlain(autoSnapshot(actions, dead))(spooky)
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
  val realBacktrace: ArrayBuffer[Action] = ArrayBuffer() //real one excluding buffered

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
      case t: Throwable => throw t
    }finally{
      super.finalize()
    }
  }

  //lazy execution by default.
  def +=(action: Action): Unit = {
    val uid = PageUID(this.backtrace :+ action)

    if (action.mayExport()) {
      //always try to read from cache first
      val restored = if (autoRestore) {
        try {
          Page.autoRestoreLatest(uid, spooky)
        }
        catch {
          case e: ObjectStreamException =>
            LoggerFactory.getLogger(this.getClass).warn("cached page(s) cannot be deserialized", e)
            null
          case e: ReflectiveOperationException =>
            LoggerFactory.getLogger(this.getClass).warn("cached page(s) cannot be deserialized", e)
            null
        }
      }
      else null

      if (restored != null) {
        pages ++= restored

        LoggerFactory.getLogger(this.getClass).info("cached page(s) found, won't go online")
      }
      else {
        for (buffered <- this.buffer)
        {
          pages ++= buffered.exe(this)() // I know buffered one should only have empty result, just for safety
          this.realBacktrace ++= buffered.trunk()
        }
        buffer.clear()
        var batch = action.exe(this)()

        if (autoSave) batch = batch.map(_.autoSave(spooky))
        if (autoCache) Page.autoCache(batch, uid,spooky)

        pages ++= batch
        this.realBacktrace ++= action.trunk()
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