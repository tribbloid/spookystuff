package com.tribbloids.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{Const, SpookyContext, SpookyException}
import org.apache.spark.TaskContext
import org.apache.spark.ml.dsl.utils.{StructRelay, StructRepr}
import org.openqa.selenium.{Dimension, NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

case object SessionRelay extends StructRelay[Session] {

  case class Repr(
                   startTime: Long,
                   backtrace: Seq[Action],
                   TaskContext: Map[String, Int]
                 ) extends StructRepr[Session] {

    override def toSelf: Session = ???
  }

  override def toRepr(v: Session): Repr = {
    ???
  }
}

abstract class Session(val spooky: SpookyContext) {

  spooky.metrics.sessionInitialized += 1
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  def webDriver: WebDriver
  def pythonDriver: PythonDriver

  //TaskContext is unreachable in withDeadline or other new threads
  val tcOpt: Option[TaskContext] = Option(TaskContext.get()) //TODO: move to constructor

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

object NoWebDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")
object NoPythonDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")

class DriverSession(
                     override val spooky: SpookyContext
                   ) extends Session(spooky){

  var webDriverOpt: Option[WebDriver] = None
  def webDriver = webDriverOpt.getOrElse{
    throw NoWebDriverException
  }

  def initializeWebDriver(): Unit = {
    SpookyUtils.retry(Const.localResourceLocalRetries) {

      SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
        val driver = spooky.conf.webDriverFactory.get(this)
        spooky.metrics.webDriverDispatched += 1
        //      try {
        driver.manage().timeouts()
          .implicitlyWait(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
          .pageLoadTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
          .setScriptTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)

        val resolution = spooky.conf.browserResolution
        if (resolution != null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

        webDriverOpt = Some(driver)
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
  }

  var pythonDriverOpt: Option[PythonDriver] = None
  def pythonDriver = pythonDriverOpt.getOrElse{
    throw NoPythonDriverException
  }

  def initializePythonDriver(): Unit = {
    SpookyUtils.retry(Const.localResourceLocalRetries) {

      SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
        val driver = spooky.conf.pythonDriverFactory.get(this)
        spooky.metrics.pythonDriverDispatched += 1

        this.pythonDriverOpt = Some(driver)
      }
    }
  }

  // can only initialize one driver, an action is less likely to use multiple drivers
  def initializeDriverIfMissing[T](f: => T): T = {
    try {
      f
    }
    catch {
      case NoWebDriverException =>
        LoggerFactory.getLogger(this.getClass).info("Initialization WebDriver ...")
        initializeWebDriver()
        f
      case NoPythonDriverException =>
        LoggerFactory.getLogger(this.getClass).info("Initialization PythonDriver ...")
        initializePythonDriver()
        f
    }
  }

  override def close(): Unit = {
    super.close()
    Option(spooky.conf.webDriverFactory).foreach{
      factory =>
        factory.release(this)
        spooky.metrics.webDriverReleased += 1
    }
    Option(spooky.conf.pythonDriverFactory).foreach{
      factory =>
        factory.release(this)
        spooky.metrics.pythonDriverReleased += 1
    }
  }
}