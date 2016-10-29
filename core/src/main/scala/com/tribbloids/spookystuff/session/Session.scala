package com.tribbloids.spookystuff.session

import java.util.Date
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.actions._
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

  val taskOpt: Option[TaskContext] = taskOrThread.toEither.left.toOption
  val taskOrThreadID = taskOrThread.id

  override def _clean(): Unit = {
    spooky.metrics.sessionReclaimed += 1
  }
}

object NoWebDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")
object NoPythonDriverException extends SpookyException("INTERNAL ERROR: should initialize driver automatically")

class DriverSession(
                     override val spooky: SpookyContext,
                     override val taskOrThread: TaskThreadInfo = TaskThreadInfo()
                   ) extends Session(spooky){

  @volatile var webDriverOpt: Option[CleanWebDriver] = None
  //throwing error instead of lazy creation is required for restarting timer
  def webDriver = webDriverOpt.getOrElse{
    throw NoWebDriverException
  }

  lazy val getOrCreateWebDriver: CleanWebDriver = {
    webDriverOpt.getOrElse {
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
          driver
        }
      }
    }
  }

  @volatile var pythonDriverOpt: Option[PythonDriver] = None
  //throwing error instead of lazy creation is required for restarting timer
  def pythonDriver = pythonDriverOpt.getOrElse{
    throw NoPythonDriverException
  }

  lazy val getOrCreatePythonDriver: PythonDriver = {
    pythonDriverOpt.getOrElse {
      SpookyUtils.retry(Const.localResourceLocalRetries) {

        SpookyUtils.withDeadline(Const.sessionInitializationTimeout) {
          val driver = spooky.conf.pythonDriverFactory.get(this)
          spooky.metrics.pythonDriverDispatched += 1

          this.pythonDriverOpt = Some(driver)
          driver
        }
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
        getOrCreateWebDriver
        f
      case NoPythonDriverException =>
        getOrCreatePythonDriver
        f
    }
  }

  override def _clean(): Unit = {
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
    super._clean()
  }
}