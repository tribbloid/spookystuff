package com.tribbloids.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.openqa.selenium.{Dimension, NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

//TODO: this should be minimized and delegated to resource pool
abstract class Session(val spooky: SpookyContext) {

  spooky.metrics.sessionInitialized += 1
  //  println("++++SESSION CREATED++++")
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  val webDriver: WebDriver

  def close(): Unit = {
    spooky.metrics.sessionReclaimed += 1
  }

  override def finalize(): Unit = {
    try {
      this.close()
      LoggerFactory.getLogger(this.getClass).info("Session is finalized by GC")
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn("!!!!! FAIL TO CLEAN UP SESSION !!!!!" + e)
    }
    finally {
      super.finalize()
    }

    //  TODO: Runtime.getRuntime.addShutdownHook()
  }
}

class DriverSession(
                     override val spooky: SpookyContext
                     //                     actionLike: ActionLike //Enable this if per row DriverFactory is used
                   ) extends Session(spooky){

  override val webDriver: WebDriver = SpookyUtils.retry(Const.localResourceLocalRetries){

    SpookyUtils.withDeadline(Const.sessionInitializationTimeout){
      var successful = false
      val driver = spooky.conf.webDriverFactory.get(this)
      spooky.metrics.driverGet += 1
      //      try {
      driver.manage().timeouts()
        .implicitlyWait(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
        .pageLoadTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
        .setScriptTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)

      val resolution = spooky.conf.browserResolution
      if (resolution != null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

      successful = true

      driver
      //      }            //TODO: these are no longer required, if a driver is get for multiple times the previous one will be automatically scuttled
      //      finally {
      //        if (!successful){
      //          driver.close()
      //          driver.quit()
      //          spooky.metrics.driverReleased += 1
      //        }
      //      }
    }
  }

  //  override val pythonDriver: SocketDriver

  override def close(): Unit = {
    spooky.conf.webDriverFactory.release(this)
    spooky.metrics.driverReleased += 1
    super.close()
  }
}

class NoDriverSession(override val spooky: SpookyContext) extends Session(spooky) {

  override val webDriver: WebDriver = null
}