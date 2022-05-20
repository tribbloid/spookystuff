package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.doc.{Doc, Unstructured}
import com.tribbloids.spookystuff.execution.SpookyExecutionContext
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{SpookySchema, SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.session.DriverLike
import com.tribbloids.spookystuff.testutils.{FunSpecx, RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils, Retry, SparkUISupport}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.jutils.jprocesses.JProcesses
import org.jutils.jprocesses.model.ProcessInfo
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.language.implicitConversions

object SpookyEnvFixture {

  import scala.collection.JavaConverters._

  //  def cleanDriverInstances(): Unit = {
  //    CleanMixin.unclean.foreach {
  //      tuple =>
  //        tuple._2.foreach (_.finalize())
  //        Predef.assert(tuple._2.isEmpty)
  //    }
  //  }

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
        val taskCleanable = tuple._2.values
          .filter { v =>
            v.lifespan.leaves.exists { ll =>
              ll._type == Lifespan.Task
            }
          }
        Predef.assert(
          taskCleanable.isEmpty,
          s": ${tuple._1} is unclean! ${taskCleanable.size} left:\n" + taskCleanable.mkString("\n")
        )
      }
  }

  def getProcesses: Seq[ProcessInfo] = Retry.FixedInterval(5, 1000) {
    JProcesses.getProcessList().asScala
  }

  /**
    * slow
    */
  def processShouldBeClean(
      conditions: Seq[ProcessInfo => Boolean] = Nil,
      cleanSweepDrivers: Boolean = true
  ): Unit = {

    if (cleanSweepDrivers) {
      //this is necessary as each suite won't automatically cleanup drivers NOT in task when finished
      Cleanable.All.cleanSweep(
        condition = {
          case _: DriverLike => true
          case _             => false
        }
      )
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

  trait EnvBase {

    def sc: SparkContext = TestHelper.TestSC
    def sql: SQLContext = TestHelper.TestSQL

    var _spooky: SpookyContext = _
    def spooky: SpookyContext = {
      Option(_spooky)
        .getOrElse {
          val result: SpookyContext = reloadSpooky
          result
        }
    }

    def reloadSpooky: SpookyContext = {
      val sql = this.sql
      val result = SpookyContext(sql, SpookyConf.default)
      _spooky = result
      result
    }

    def spookyConf: SpookyConf = spooky.getConf(Core)
  }

}

abstract class SpookyEnvFixture
    extends FunSpecx
    with SpookyEnvFixture.EnvBase
    with RemoteDocsFixture
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Retries
    with SparkUISupport {

  val exitingPIDs: Set[String] = SpookyEnvFixture.getProcesses.map(_.getPid).toSet

  def parallelism: Int = sc.defaultParallelism

  lazy val defaultEC: SpookyExecutionContext = SpookyExecutionContext(spooky)
  lazy val defaultSchema: SpookySchema = SpookySchema(defaultEC)

  def emptySchema: SpookySchema = SpookySchema(SpookyExecutionContext(spooky))

  implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WSchema = row.WSchema(emptySchema)
  implicit def extractor2Resolved[T, R](extractor: Alias[T, R]): GenResolved[T, R] = GenResolved(
    extractor.resolve(emptySchema),
    TypedField(
      extractor.field,
      extractor.resolveType(emptySchema)
    )
  )
  implicit def extractor2Function[T, R](extractor: GenExtractor[T, R]): PartialFunction[T, R] =
    extractor.resolve(emptySchema)
  implicit def doc2Root(doc: Doc): Unstructured = doc.root

  override def withFixture(test: NoArgTest): Outcome = {
    if (isRetryable(test))
      CommonUtils.retry(4) { super.withFixture(test) } else
      super.withFixture(test)
  }

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def _processNames: Seq[String] = Seq("phantomjs", s"${PythonDriverFactory.python3} -iu")
  final lazy val conditions = {
    val _processNames = this._processNames
    val exitingPIDs = this.exitingPIDs
    _processNames.map { name =>
      { process: ProcessInfo =>
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

    CommonUtils.retry(3, 1000) {
      sc.runEverywhere() { _ =>
        SpookyEnvFixture.shouldBeClean(spooky, conditions)
      }
    }

    SpookyEnvFixture.firstRun = false
  }

  override def beforeAll(): Unit = {

    super.beforeAll()

    if (SpookyEnvFixture.firstRun)
      validateBeforeAndAfterAll()
  }

  override def afterAll(): Unit = {

    validateBeforeAndAfterAll()

    super.afterAll()

  }

  override def beforeEach(): Unit = CommonUtils.retry(3, 1000) {
    //    SpookyEnvFixture.cleanDriverInstances()
    spooky.spookyMetrics.resetAll()

    spooky.setConf(
      SpookyConf(
        cacheWrite = false,
        cacheRead = false
      ),
      Dir.Conf(
        root = CommonUtils.\\\(CommonConst.USER_TEMP_DIR, "spooky-unit")
      )
    )
  }

  override def afterEach(): Unit = {
    val spooky = this.spooky
    sc.runEverywhere() { _ =>
      SpookyEnvFixture.instancesShouldBeClean(spooky)
    }
  }
}
