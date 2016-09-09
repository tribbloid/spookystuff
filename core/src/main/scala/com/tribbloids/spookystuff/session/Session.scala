package com.tribbloids.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{Const, SpookyContext, SpookyException}
import org.apache.spark.TaskContext
import org.apache.spark.ml.dsl.utils.{Message, MessageRelay}
import org.openqa.selenium.{Dimension, NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

case object SessionRelay extends MessageRelay[Session] {

  case class M(
                startTime: Long,
                backtrace: Seq[Action],
                TaskContext: Option[TaskContextRelay.M]
              ) extends Message

  override def toMessage(v: Session): M = {
    M(
      v.startTime,
      v.backtrace,
      v.taskContextOpt.map (tc => TaskContextRelay.toMessage(tc))
    )
  }
}

case object TaskContextRelay extends MessageRelay[TaskContext] {

  case class M(
                attemptNumber: Int
              ) extends Message

  override def toMessage(v: TaskContext): M = {
    M(
      v.attemptNumber()
    )
  }
}

abstract class Session(val spooky: SpookyContext) extends CleanMixin {

  spooky.metrics.sessionInitialized += 1
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  def webDriver: WebDriver
  def pythonDriver: PythonDriver

  //TaskContext is unreachable in withDeadline or other new threads
  val taskContextOpt: Option[TaskContext] = Option(TaskContext.get()) //TODO: move to constructor

  def close(): Unit = {
    spooky.metrics.sessionReclaimed += 1
  }

  override def clean(): Unit = {
    this.close()
    LoggerFactory.getLogger(this.getClass).info("Session is finalized by GC")
  }
}

object NoWebDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")
object NoPythonDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")

class DriverSession(
                     override val spooky: SpookyContext
                   ) extends Session(spooky){

  @volatile var webDriverOpt: Option[WebDriver] = None
  //throwing error instead of lazy creation is required for restarting timer
  def webDriver = webDriverOpt.getOrElse{
    throw NoWebDriverException
  }

  def initializeWebDriverIfMissing(): Unit = {
    if (webDriverOpt.nonEmpty) return
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

  @volatile var pythonDriverOpt: Option[PythonDriver] = None
  //throwing error instead of lazy creation is required for restarting timer
  def pythonDriver = pythonDriverOpt.getOrElse{
    throw NoPythonDriverException
  }

  def initializePythonDriverIfMissing(): Unit = {
    if (pythonDriverOpt.nonEmpty) return
    SpookyUtils.retry(Const.localResourceLocalRetries) {

      SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
        val driver = spooky.conf.pythonDriverFactory.get(this)
        spooky.metrics.pythonDriverDispatched += 1

        this.pythonDriverOpt = Some(driver)
      }
    }
  }

  // TODO: can only initialize one driver, an action is less likely to use multiple drivers
  def initializeDriverIfMissing[T](f: => T): T = {
    try {
      f
    }
    catch {
      case NoWebDriverException =>
        LoggerFactory.getLogger(this.getClass).info("Initialization WebDriver ...")
        initializeWebDriverIfMissing()
        f
      case NoPythonDriverException =>
        LoggerFactory.getLogger(this.getClass).info("Initialization PythonDriver ...")
        initializePythonDriverIfMissing()
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