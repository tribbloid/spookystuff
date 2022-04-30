/*
Copyright 2007-2010 Selenium committers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.session.{DriverLike, Session}
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan}
import com.tribbloids.spookystuff.{DriverStatus, SpookyContext}
import org.apache.spark.TaskContext

//local to TaskID, if not exist, local to ThreadID
//for every new driver created, add a taskCompletion listener that salvage it.
//TODO: get(session) should have 2 impl:
// if from the same session release the existing one immediately.
// if from a different session but same taskAttempt wait for the old one to be released.
// in any case it should ensure 1 taskAttempt only has 1 active driver
//TODO: delay Future-based waiting control until asynchronous Action exe is implemented. Right now it works just fine
sealed abstract class DriverFactory[D <: DriverLike] extends Serializable {

  // If get is called again before the previous driver is released, the old driver is destroyed to create a new one.
  // this is to facilitate multiple retries

  def dispatch(session: Session): D

  // release all Drivers that belong to a session
  def release(session: Session): Unit

  def driverLifespan(session: Session): Lifespan = Lifespan.TaskOrJVM(ctxFactory = () => session.lifespan.ctx)

  def deployGlobally(spooky: SpookyContext): Unit = {}
}

object DriverFactory {

  /**
    * session local
    */
  abstract class Transient[D <: DriverLike] extends DriverFactory[D] with Product {

    override lazy val toString: String = productPrefix

    // session -> driver
    // cleanup: this has no effect whatsoever
    @transient lazy val sessionLocals: ConcurrentMap[Session, D] = ConcurrentMap()

    def dispatch(session: Session): D = {
      release(session)
      val driver = create(session)
      sessionLocals += session -> driver
      driver
    }

    final def create(session: Session): D = {
      _createImpl(session, driverLifespan(session))
    }

    def _createImpl(session: Session, lifespan: Lifespan): D

    def factoryReset(driver: D): Unit

    def release(session: Session): Unit = {
      val existingOpt = sessionLocals.remove(session)
      existingOpt.foreach { driver =>
        destroy(driver, session.taskContextOpt)
      }
    }

    final def destroy(driver: D, tcOpt: Option[TaskContext]): Unit = {
      driver match {
        case v: Cleanable => v.tryClean()
        case _            =>
      }
    }

    final lazy val taskLocal = TaskLocal(this)
  }

  /**
    * delegate create & destroy to PerSessionFactory
    * first get() create a driver as usual
    * calling get() without release() reboot the driver
    * first release() return driver to the pool to be used by the same Spark Task
    * call any function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
    */
  case class TaskLocal[D <: DriverLike](
      delegate: Transient[D]
  ) extends DriverFactory[D] {

    //taskOrThreadIDs -> (driver, busy)
    @transient lazy val taskLocals: ConcurrentMap[Seq[Any], DriverStatus[D]] = {
      ConcurrentMap()
    }

    override def dispatch(session: Session): D = {

      val ls = driverLifespan(session)
      val taskLocalOpt = taskLocals.get(ls.batchIDs)

      def newDriver: D = {
        val fresh = delegate.create(session)
        taskLocals.put(ls.batchIDs, new DriverStatus(fresh))
        fresh
      }

      taskLocalOpt
        .map { status =>
          def recreateDriver: D = {
            delegate.destroy(status.self, session.taskContextOpt)
            newDriver
          }

          if (!status.isBusy) {
            try {
              delegate.factoryReset(status.self)
              status.isBusy = true
              status.self
            } catch {
              case _: Exception =>
                recreateDriver
            }
          } else {
            // TODO: should wait until its no longer busy, instead of destroying it.
            recreateDriver
          }
        }
        .getOrElse {
          newDriver
        }
    }

    override def release(session: Session): Unit = {

      val ls = driverLifespan(session)
      val statusOpt = taskLocals.get(ls.batchIDs)
      statusOpt.foreach { status =>
        status.isBusy = false
      }
    }

    override def deployGlobally(spooky: SpookyContext): Unit = delegate.deployGlobally(spooky)
  }

  ////just for debugging
  ////a bug in this driver has caused it unusable in Firefox 32
  //object FirefoxDriverFactory extends DriverFactory {
  //
  //  val baseCaps = new DesiredCapabilities
  //  //  baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  //  //  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
  //
  //  //  val FirefoxRootPath = "/usr/lib/phantomjs/"
  //  //  baseCaps.setCapability("webdriver.firefox.bin", "firefox");
  //  //  baseCaps.setCapability("webdriver.firefox.profile", "WebDriver");
  //
  //  override def newInstance(capabilities: Capabilities, spooky: SpookyContext): WebDriver = {
  //    val newCap = baseCaps.merge(capabilities)
  //
  //    Utils.retry(Const.DFSInPartitionRetry) {
  //      Utils.withDeadline(spooky.distributedResourceTimeout) {new FirefoxDriver(newCap)}
  //    }
  //  }
  //}

}
