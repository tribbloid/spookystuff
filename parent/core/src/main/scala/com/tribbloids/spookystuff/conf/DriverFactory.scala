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

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.agent.{Agent, DriverLike, DriverStatus}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.{BatchID, Lifespan}
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

  def dispatch(agent: Agent): D

  // release all Drivers that belong to a session
  def release(agent: Agent): Unit

  def driverLifespan(agent: Agent): Lifespan =
    Lifespan.TaskOrJVM(ctxFactory = () => agent.lifespan.ctx).forShipping

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
    @transient lazy val sessionLocals: ConcurrentMap[Agent, D] = ConcurrentMap()

    def dispatch(agent: Agent): D = {
      release(agent)
      val driver = create(agent)
      sessionLocals += agent -> driver
      driver
    }

    final def create(agent: Agent): D = {
      _createImpl(agent, driverLifespan(agent))
    }

    def _createImpl(agent: Agent, lifespan: Lifespan): D

    def factoryReset(driver: D): Unit

    def release(agent: Agent): Unit = {
      val existingOpt = sessionLocals.remove(agent)
      existingOpt.foreach { driver =>
        destroy(driver, agent.taskContextOpt)
      }
    }

    final def destroy(driver: D, tcOpt: Option[TaskContext]): Unit = {

      driver.clean()
    }

    final lazy val taskLocal: TaskLocal[D] = TaskLocal(this)
  }

  /**
    * delegate create & destroy to PerSessionFactory first get() create a driver as usual calling get() without
    * release() reboot the driver first release() return driver to the pool to be used by the same Spark Task call any
    * function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
    */
  case class TaskLocal[D <: DriverLike](
      delegate: Transient[D]
  ) extends DriverFactory[D] {

    // taskOrThreadIDs -> (driver, busy)
    @transient lazy val taskLocals: ConcurrentMap[Seq[BatchID], DriverStatus[D]] = {
      ConcurrentMap()
    }

    override def dispatch(agent: Agent): D = {

      val ls = driverLifespan(agent)
      val taskLocalOpt = taskLocals.get(ls.registeredIDs)

      def newDriver: D = {
        val fresh = delegate.create(agent)
        taskLocals.put(ls.registeredIDs, new DriverStatus(fresh))
        fresh
      }

      taskLocalOpt
        .map { status =>
          def recreateDriver: D = {
            delegate.destroy(status.self, agent.taskContextOpt)
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

    override def release(agent: Agent): Unit = {

      val ls = driverLifespan(agent)
      val statusOpt = taskLocals.get(ls.registeredIDs)
      statusOpt.foreach { status =>
        status.isBusy = false
      }
    }

    override def deployGlobally(spooky: SpookyContext): Unit = delegate.deployGlobally(spooky)
  }
}
