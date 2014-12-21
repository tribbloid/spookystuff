package org.tribbloid.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import org.openqa.selenium.Dimension
import org.openqa.selenium.remote.SessionNotFoundException
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.{Const, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.mutable.ArrayBuffer

//TODO: this should be minimized and delegated to resource pool
abstract class Session(val spooky: SpookyContext) {

  spooky.metrics.sessionInitialized += 1
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  val driver: CleanWebDriver

  def close(): Unit = {
    spooky.metrics.sessionReclaimed += 1
  }

  override def finalize(): Unit = {
    try {
      this.close()
      LoggerFactory.getLogger(this.getClass).info("Session is finalized by GC")
    }
    catch {
      case e: SessionNotFoundException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn("!!!!!FAIL TO CLEAN UP SESSION!!!!!" + e)
    }
    finally {
      super.finalize()
    }

    //  TODO: Runtime.getRuntime.addShutdownHook()
  }
}

class DriverSession(override val spooky: SpookyContext) extends Session(spooky){

  override val driver: CleanWebDriver = Utils.retry(Const.localResourceLocalRetry){
    Utils.withDeadline(Const.sessionInitializationTimeout){
      var successful = false
      val driver = spooky.driverFactory.newInstance(null, spooky)
      spooky.metrics.driverInitialized += 1
      try {
        driver.manage().timeouts()
          .implicitlyWait(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
          .pageLoadTimeout(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
          .setScriptTimeout(spooky.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)

        val resolution = spooky.browserResolution
        if (resolution != null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

        successful = true

        driver
      }
      finally {
        if (!successful){
          driver.close()
          driver.quit()
          spooky.metrics.driverReclaimed += 1
        }
      }
    }
  }

  override def close(): Unit = {
    driver.close()
    driver.quit()
    spooky.metrics.driverReclaimed += 1
    super.close()
  }
}

class NoDriverSession(override val spooky: SpookyContext) extends Session(spooky) {

  override val driver: CleanWebDriver = null
}