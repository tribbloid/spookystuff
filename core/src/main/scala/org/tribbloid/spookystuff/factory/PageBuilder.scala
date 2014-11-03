package org.tribbloid.spookystuff.factory

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

    val pb = new PageBuilder(spooky)

    try {
      pb ++= actions

      pb.pages
    }
    finally {
      pb.close()
    }
  }
}

class PageBuilder(
                   val spooky: SpookyContext,
                   caps: Capabilities = null,
                   val startTime: Long = new Date().getTime
                   ){

  val autoSave = spooky.autoSave
  val autoCache = spooky.autoCache
  val autoRestore = spooky.autoRestore

  @volatile
  private var _driver: WebDriver = null

  //mimic lazy val but retain ability to destruct it on demand
  //not thread safe?
  def driver: WebDriver = {
    if (_driver == null) _driver = spooky.driverFactory.newInstance(caps, spooky)
    _driver
  }

  val backtrace: ArrayBuffer[Action] = ArrayBuffer() //kept in session, but it is action deciding if they fit in or what part to export
  val realBacktrace: ArrayBuffer[Action] = ArrayBuffer() //real one excluding buffered

  private val buffer: ArrayBuffer[Action] = ArrayBuffer()
  val pages: ArrayBuffer[Page] = ArrayBuffer()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)


  //remember to call this! don't want thousands of phantomJS browsers opened
  def close(): Unit = {
    if (_driver != null) {
      _driver.close()
      _driver.quit()
    }
  }

  override def finalize(): Unit = {
    try {
      LoggerFactory.getLogger(this.getClass).warn("FINALIZER THE ULTIMATE EVIL WAS SUMMONED!!!!!!")
      this.close() //this is greate evil, make sure it is never called by normal means
    }
    finally{
      super.finalize()
    }
  }

  //lazy execution by default.
  def +=(action: Action): Unit = {
    val uid = PageUID(this.backtrace :+ action)

    this.backtrace ++= action.trunk()//always put into backtrace.
    if (action.mayExport()) {
      //always try to read from cache first
      val restored = if (autoRestore) {

          Page.autoRestoreLatest(uid, spooky)
      }
      else null

      if (restored != null) {
        pages ++= restored

        LoggerFactory.getLogger(this.getClass).info("cached page(s) found, won't go online")
      }
      else {
        for (buffered <- this.buffer)
        {
          this.realBacktrace ++= buffered.trunk()
          pages ++= buffered.exe(this)() // I know buffered one should only have empty result, just for safety
        }
        buffer.clear()

        this.realBacktrace ++= action.trunk()
        var batch = action.exe(this)()

        if (autoSave) batch = batch.map(_.autoSave(spooky))
        if (autoCache) Page.autoCache(batch, uid,spooky)

        pages ++= batch
      }
    }
    else {
      this.buffer += action
    }
  }

  def ++= (actions: Seq[Action]): Unit = {
    actions.foreach(this += _)
  }
}