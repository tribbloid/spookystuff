package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark.{Envs, TestHelper}
import ai.acyclic.prover.commons.util.Retry
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.agent.DriverLike
import com.tribbloids.spookystuff.commons.lifespan.Cleanable
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeThrowable}
import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.doc.{Doc, Unstructured}
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.{SpookySchema, SquashedRow}
import org.jutils.jprocesses.JProcesses
import org.jutils.jprocesses.model.ProcessInfo
import org.scalatest.{BeforeAndAfterEach, Outcome, Retries}

import scala.language.implicitConversions
import scala.util.Try

object SpookyBaseSpec {

  import scala.jdk.CollectionConverters._

  @volatile var firstRun: Boolean = true

  def shouldBeClean(
      spooky: SpookyContext,
      conditions: Seq[ProcessInfo => Boolean]
  ): Unit = {

    instancesShouldBeClean(spooky)
    processShouldBeClean(conditions)
  }

  def instancesShouldBeClean(spooky: SpookyContext): Unit = {

    Cleanable.uncleaned
      .foreach { tuple =>
        val taskCleanable = tuple._2.active.values
          .filter { v =>
            val isOfTask = v.lifespan.leaves.exists { ll =>
              ll._type == Lifespan.Task
            }
            val isNotCleaned = !v.isCleaned
            isOfTask && isNotCleaned
          }
        Predef.assert(
          taskCleanable.isEmpty,
          s": ${tuple._1} is unclean! ${taskCleanable.size} left:\n" + taskCleanable.mkString("\n")
        )
      }
  }

  def getProcesses: Seq[ProcessInfo] = Retry.FixedInterval(5, 1000) {
    JProcesses.getProcessList().asScala.toSeq
  }

  /**
    * slow
    */
  def processShouldBeClean(
      conditions: Seq[ProcessInfo => Boolean] = Nil,
      cleanSweepDrivers: Boolean = true
  ): Unit = {

    if (cleanSweepDrivers) {
      // this is necessary as each suite won't automatically cleanup drivers NOT in task when finished
      Cleanable.All
        .filter {

          case _: DriverLike => true
          case _             => false
        }
        .cleanSweep()
    }

    conditions.foreach { condition =>
      val matchedProcess = getProcesses.filter { v =>
        condition(v)
      }
      Predef.assert(
        matchedProcess.isEmpty,
        s"${matchedProcess.size} process(es) left:\n" + matchedProcess.mkString("\n")
      )
    }
  }

}

abstract class SpookyBaseSpec extends SpookyEnvSpec with BeforeAndAfterEach with Retries {

  lazy val defaultEC: ExecutionContext = ExecutionContext(spooky)
  lazy val defaultSchema: SpookySchema = SpookySchema(defaultEC)

  def emptySchema: SpookySchema = SpookySchema(ExecutionContext(spooky))

  implicit def rowWithCtx(row: SquashedRow[?]): row._WithCtx = row.withCtx(spooky)

  implicit def doc2Root(doc: Doc): Unstructured = doc.root

  override def withFixture(test: NoArgTest): Outcome = {
    if (isRetryable(test))
      CommonUtils.retry(4)(super.withFixture(test))
    else
      super.withFixture(test)
  }

  import com.tribbloids.spookystuff.utils.RDDImplicits._

  def _externalProcessNames: Seq[String] = Seq("phantomjs", s"${PythonDriverFactory.python3} -iu")
  val exitingPIDs: Set[String] = SpookyBaseSpec.getProcesses.map(_.getPid).toSet
  final lazy val conditions: Seq[ProcessInfo => Boolean] = {
    val _processNames = this._externalProcessNames
    val exitingPIDs = this.exitingPIDs
    _processNames.map { name =>
      { (process: ProcessInfo) =>
        val c1 = process.getName == name
        val c2 = !exitingPIDs.contains(process.getPid)
        c1 && c2
      }
    }
  }

  def validateBeforeAndAfterAll(): Unit = {

    TestHelper.cleanTempDirs()

    val spooky = this.spooky
    val conditions = this.conditions
    val result = sc.runEverywhere() { _ =>
      Try {
        CommonUtils.retry(3, 1000, silent = true) {
          SpookyBaseSpec.shouldBeClean(spooky, conditions)
        }
      }
    }
    TreeThrowable.&&&(result)

    SpookyBaseSpec.firstRun = false
  }

  override def beforeAll(): Unit = {

    super.beforeAll()

    if (SpookyBaseSpec.firstRun)
      validateBeforeAndAfterAll()
  }

  override def afterAll(): Unit = {

    validateBeforeAndAfterAll()

    super.afterAll()

  }

  override def beforeEach(): Unit = CommonUtils.retry(3, 1000, silent = true) {
    //    SpookyEnvFixture.cleanDriverInstances()
    spooky.spookyMetrics.resetAll()

    spooky.setConf(
      SpookyConf(
        cacheWrite = false,
        cacheRead = false
      ),
      Dir.Conf(
        root = Envs.USER_TEMP_DIR :\ "spooky-unit"
      )
    )
  }

  override def afterEach(): Unit = {
    val spooky = this.spooky
    val result = sc.runEverywhere() { _ =>
      Try {

        CommonUtils.retry(3, 1000, silent = true) {
          SpookyBaseSpec.instancesShouldBeClean(spooky)
        }
      }
    }
    TreeThrowable.&&&(result)
  }
}
