package org.tribbloid.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import org.openqa.selenium.Dimension
import org.openqa.selenium.remote.SessionNotFoundException
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.pages.{NoPage, PageLike, PageUtils, Page}

import scala.collection.mutable.ArrayBuffer

//TODO: this should be minimized and delegated to resource pool
class Session(val spooky: SpookyContext){

  spooky.metrics.sessionInitialized += 1

  val startTime: Long = new Date().getTime

  @volatile
  private var _driver: CleanWebDriver = null

  def existingDriver: Option[CleanWebDriver] = Option(_driver)

  //mimic lazy val but retain ability to destruct it on demandairframe
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

  def close(): Unit = {
    spooky.metrics.sessionReclaimed += 1
    if (_driver != null) {
      _driver.close()
      _driver.quit()
      spooky.metrics.driverReclaimed += 1
    }
  }

  override def finalize(): Unit = {
    try {
      this.close()
      LoggerFactory.getLogger(this.getClass).info("Session is finalized by GC")
    }
    catch {
      case e: SessionNotFoundException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn("!!!!!FAIL TO CLEAN UP SESSION!!!!!"+e)
    }
    finally{
      super.finalize()
    }
  }

  val backtrace: ArrayBuffer[Action] = ArrayBuffer() //kept in session, but it is action deciding if they fit in or what part to export
  val realBacktrace: ArrayBuffer[Action] = ArrayBuffer() //real one excluding buffered

  private val buffer: ArrayBuffer[Action] = ArrayBuffer()
  val pageLikes: ArrayBuffer[PageLike] = ArrayBuffer()

  //  TODO: Runtime.getRuntime.addShutdownHook()
  //by default drivers should be reset and reused in this case, but whatever

  //  def exe(action: Action): Array[Page] = action.exe(this)

  //lazy execution by default.
  def +=(action: Action): Unit = {
    val trace = Trace(this.backtrace :+ action)//TODO: this shouldn't happen

    this.backtrace ++= action.trunk//always put into backtrace.
    if (action.mayExport) {
      //always try to read from cache first
      val restored = if (spooky.autoRestore) {

        PageUtils.autoRestoreLatest(trace, spooky)
      }
      else null

      if (restored != null) {
        pageLikes ++= restored

        LoggerFactory.getLogger(this.getClass).info("cached page(s) found, won't go online")
      }
      else {
        for (buffered <- this.buffer)
        {
//          pages.clear()//TODO: need to save later version if buffered is refreshed
          buffered(this) // buffered one may have non-empty results that are restored before
          this.realBacktrace ++= buffered.trunk
        }
        buffer.clear()

        var batch = action(this)
        this.realBacktrace ++= action.trunk

        if (spooky.autoSave) batch.foreach{
          case page: Page => page.autoSave(spooky)
          case _ =>
        }
        if (spooky.autoCache) PageUtils.autoCache(batch, spooky)

        pageLikes ++= batch
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