package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.doc.{Doc, Unstructured}
import com.tribbloids.spookystuff.dsl.DriverFactory
import com.tribbloids.spookystuff.execution.SpookyExecutionContext
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{SpookySchema, SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.session.{CleanWebDriver, Driver}
import com.tribbloids.spookystuff.testutils.{FunSpecx, RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan}
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils, RetryFixedInterval}
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
        val nonLocalDrivers = tuple._2.values
          .filter { v =>
            v.lifespan.isInstanceOf[Lifespan.Task]
          }
        Predef.assert(
          nonLocalDrivers.isEmpty,
          s": ${tuple._1} is unclean! ${nonLocalDrivers.size} left:\n" + nonLocalDrivers.mkString("\n")
        )
      }
  }

  def getProcesses: Seq[ProcessInfo] = RetryFixedInterval(5, 1000) {
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
      Cleanable.cleanSweepAll(
        condition = {
          case v: Driver => true
          case _         => false
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

    lazy val driverFactory: DriverFactory[CleanWebDriver] = SpookyConf.TEST_WEBDRIVER_FACTORY

    @transient lazy val spookyConf = new SpookyConf(
      webDriverFactory = driverFactory
    )
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
      val result = new SpookyContext(sql, spookyConf)
      _spooky = result
      result
    }
  }

}

abstract class SpookyEnvFixture
    extends FunSpecx
    with SpookyEnvFixture.EnvBase
    with RemoteDocsFixture
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Retries {

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

  def _processNames = Seq("phantomjs", "python")
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

  override def beforeAll(): Unit = if (SpookyEnvFixture.firstRun) {

    super.beforeAll()

    val spooky = this.spooky
    val conditions = this.conditions
    sc.runEverywhere() { _ =>
      SpookyEnvFixture.shouldBeClean(spooky, conditions)
    }
    SpookyEnvFixture.firstRun = false
  }

  override def afterAll() {

    val spooky = this.spooky
    val conditions = this.conditions
    TestHelper.cleanTempDirs()

    //unpersist all RDDs, disabled to better detect memory leak
    //    sc.getPersistentRDDs.values.toList.foreach {
    //      _.unpersist()
    //    }

    sc.runEverywhere() { _ =>
      SpookyEnvFixture.shouldBeClean(spooky, conditions)
    }

    super.afterAll()
  }

  override def beforeEach(): Unit = CommonUtils.retry(3, 1000) {
    //    SpookyEnvFixture.cleanDriverInstances()
    spooky._configurations = submodules
    spooky.spookyMetrics.resetAll()
    spooky.rebroadcast()
  }

  def submodules: Submodules[AbstractConf] = {
    Submodules(
      new SpookyConf(
        autoSave = true,
        cacheWrite = false,
        cacheRead = false
      ),
      DirConf(
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
