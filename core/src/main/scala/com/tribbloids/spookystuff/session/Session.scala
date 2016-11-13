package com.tribbloids.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.utils.{NOTSerializable, SpookyUtils}
import com.tribbloids.spookystuff.{Const, SpookyContext, SpookyException}
import org.apache.spark.TaskContext
import org.apache.spark.ml.dsl.utils.{Message, MessageRelay}
import org.openqa.selenium.Dimension
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
      v.taskOpt.map (tc => TaskContextRelay.toMessage(tc))
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

abstract class Session(val spooky: SpookyContext) extends AutoCleanable with NOTSerializable {

  spooky.metrics.sessionInitialized += 1
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  def webDriver: CleanWebDriver
  def pythonDriver: PythonDriver

  val taskOpt: Option[TaskContext] = Option(TaskContext.get())

  override def _cleanImpl(): Unit = {
    spooky.metrics.sessionReclaimed += 1
  }
}

object NoWebDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")
object NoPythonDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")

class DriverSession(
                     override val spooky: SpookyContext,
                     override val lifespan: Lifespan = Lifespan()
                   ) extends Session(spooky){

  @volatile private var _webDriverOpt: Option[CleanWebDriver] = None
  def webDriverOpt = _webDriverOpt.filter(!_.isCleaned)
  //throwing error instead of lazy creation is required for restarting timer
  def webDriver = webDriverOpt.getOrElse{
    throw NoWebDriverException
  }

  def getOrProvisionWebDriver: CleanWebDriver = {
    webDriverOpt.getOrElse {
      SpookyUtils.retry(Const.localResourceLocalRetries) {
        SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
          val driver = spooky.conf.webDriverFactory.provision(this)
          spooky.metrics.webDriverDispatched += 1
          //      try {
          driver.manage().timeouts()
            .implicitlyWait(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
            .pageLoadTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)
            .setScriptTimeout(spooky.conf.remoteResourceTimeout.toSeconds, TimeUnit.SECONDS)

          val resolution = spooky.conf.browserResolution
          if (resolution != null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

          _webDriverOpt = Some(driver)
          //      }            //TODO: these are no longer required, if a driver is get for multiple times the previous one will be automatically scuttled
          //      finally {
          //        if (!successful){
          //          driver.close()
          //          driver.quit()
          //          spooky.metrics.driverReleased += 1
          //        }
          //      }
          driver
        }
      }
    }
  }

  @volatile private var _pythonDriverOpt: Option[PythonDriver] = None
  def pythonDriverOpt = _pythonDriverOpt.filter(!_.isCleaned)
  //throwing error instead of lazy creation is required for restarting timer
  def pythonDriver = pythonDriverOpt.getOrElse{
    throw NoPythonDriverException
  }

  def getOrProvisionPythonDriver: PythonDriver = {
    pythonDriverOpt.getOrElse {
      SpookyUtils.retry(Const.localResourceLocalRetries) {

        SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
          val driver = spooky.conf.pythonDriverFactory.provision(this)
          spooky.metrics.pythonDriverDispatched += 1

          _pythonDriverOpt = Some(driver)
          driver
        }
      }
    }
  }

  def initializeDriverIfMissing[T](f: => T, n: Int = 3): T = {
    try {
      assert(n >= 0)
      f
    }
    catch {
      case NoWebDriverException =>
        LoggerFactory.getLogger(this.getClass).info(s"Web driver doesn't exist, creating ... $n time(s) left")
        getOrProvisionWebDriver
        initializeDriverIfMissing(f, n - 1)
      case NoPythonDriverException =>
        LoggerFactory.getLogger(this.getClass).info(s"Python driver doesn't exist, creating ... $n time(s) left")
        getOrProvisionPythonDriver
        initializeDriverIfMissing(f, n - 1)
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error("UNKNOWN ERROR:", e)
        throw e
    }
  }

  override def _cleanImpl(): Unit = {
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
    super._cleanImpl()
  }
}