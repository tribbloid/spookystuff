package org.tribbloid.spookystuff.factory

import java.util.Date
import java.util.concurrent.TimeUnit

import org.openqa.selenium.Dimension
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.entity._

import scala.collection.mutable.ArrayBuffer

//object PageBuilder {
//
//  def autoSnapshot(actions: Seq[Action], dead: Boolean): Seq[Action] = {
//    if (Action.mayExport(actions) || dead) actions
//    else actions :+ Snapshot() //Don't use singleton, otherwise will flush timestamp
//  }
//
//  def resolve(actions: Seq[Action], dead: Boolean)(spooky: SpookyContext): Seq[Page] = {
//
//    Utils.retry (Const.remoteResourceInPartitionRetry){
//      resolvePlain(autoSnapshot(actions, dead))(spooky)
//    }
//  }
//
//  // Major API shrink! resolveFinal will be merged here
//  // if a resolve has no potential to output page then a snapshot will be appended at the end
//  def resolvePlain(actions: Seq[Action])(spooky: SpookyContext): Seq[Page] = {
//
//    //    val results = ArrayBuffer[Page]()
//
//    val pb = new PageBuilder(spooky)
//
//    try {
//      pb ++= actions
//
//      pb.pages
//    }
//    finally {
//      pb.close()
//    }
//  }
//}

//TODO: this should be minimized and delegated to resource pool
class PageBuilder(val spooky: SpookyContext){

  val startTime: Long = new Date().getTime

  val autoSave = spooky.autoSave
  val autoCache = spooky.autoCache
  val autoRestore = spooky.autoRestore

  @volatile
  private var _driver: CleanWebDriver = null

  def existingDriver: Option[CleanWebDriver] = Option(_driver)

  //mimic lazy val but retain ability to destruct it on demand
  //TODO: make sure it can only be invoked by a subroutine with a deadline, or it will take forever!
  def getDriver: CleanWebDriver = {
    if (_driver == null) {
      _driver = spooky.driverFactory.newInstance(null, spooky)

      _driver.manage().timeouts()
        .implicitlyWait(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
        .pageLoadTimeout(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
        .setScriptTimeout(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)

      val resolution = spooky.browserResolution
      if (resolution != null) _driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))
    }
    _driver
  }

  //remember to call this! don't want thousands of phantomJS browsers opened
  def close(): Unit = {
    if (_driver != null) {
      _driver.close()
      _driver.quit()
    }
  }

  override def finalize(): Unit = {
    try {
      this.close() //this is greate evil, make sure it is never called by normal means
      LoggerFactory.getLogger(this.getClass).info("PageBuilder is finalized by GC")
    }
    finally{
      super.finalize()
    }
  }

  val backtrace: ArrayBuffer[Action] = ArrayBuffer() //kept in session, but it is action deciding if they fit in or what part to export
  val realBacktrace: ArrayBuffer[Action] = ArrayBuffer() //real one excluding buffered

  private val buffer: ArrayBuffer[Action] = ArrayBuffer()
  val pages: ArrayBuffer[Page] = ArrayBuffer()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)

  //lazy execution by default.
  def +=(action: Action): Unit = {
    val uid = PageUID(Trace(this.backtrace :+ action), null)//TODO: this shouldn't happen

    this.backtrace ++= action.trunk//always put into backtrace.
    if (action.mayExport) {
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
          this.realBacktrace ++= buffered.trunk
          pages ++= buffered.apply(this) // I know buffered one should only have empty result, just for safety
        }
        buffer.clear()

        this.realBacktrace ++= action.trunk
        var batch = action.apply(this)

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