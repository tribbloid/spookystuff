package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactory
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{DataRowSchema, SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.session.{AutoCleanable, CleanWebDriver}
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.{MultiCauses, SpookyUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.jutils.jprocesses.JProcesses
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Retries}

import scala.language.implicitConversions
import scala.util.Try

object SpookyEnvFixture {
//  def cleanDriverInstances(): Unit = {
//    CleanMixin.unclean.foreach {
//      tuple =>
//        tuple._2.foreach (_.finalize())
//        assert(tuple._2.isEmpty)
//    }
//  }

  def shouldBeClean(spooky: SpookyContext): Unit = {
    driverInstancesShouldBeClean(spooky)
    driverProcessShouldBeClean()
  }

  def driverInstancesShouldBeClean(spooky: SpookyContext): Unit = {
    AutoCleanable.cleanupLocally() //nobody cares about local leakage

    AutoCleanable.uncleaned.foreach {
      tuple =>
        val nonLocalDrivers = tuple._2
        assert(
          nonLocalDrivers.isEmpty,
          s": ${tuple._1} is unclean! ${nonLocalDrivers.size} left:\n" + nonLocalDrivers.mkString("\n")
        )
    }
  }

  /**
    * slow
    */
  def driverProcessShouldBeClean(): Unit = {
    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    val phantomJSProcesses = processes.filter(_.getName == "phantomjs")
    val pythonProcesses = processes.filter(_.getName == "python")
    MultiCauses.&&&(Seq(
      Try{assert(
        phantomJSProcesses.isEmpty,
        s"${phantomJSProcesses.size} PhantomJS processes left:\n" + phantomJSProcesses.mkString("\n")
      )},
      Try{assert(
        pythonProcesses.isEmpty,
        s"${pythonProcesses.size} Python processes left:\n" + pythonProcesses.mkString("\n")
      )}
    ))
  }
}

abstract class SpookyEnvFixture
  extends FunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Retries
    with RemoteDocsFixture {

  val startTime = System.currentTimeMillis()

  def sc: SparkContext = TestHelper.TestSpark
  def sql: SQLContext = TestHelper.TestSQL

  lazy val spookyConf = new SpookyConf(
    webDriverFactory = driverFactory
  )

  var _spooky: SpookyContext = _
  def spooky = Option(_spooky).getOrElse {
    val result = new SpookyContext(sql, spookyConf)
    _spooky = result
    result
  }

  lazy val schema = DataRowSchema(spooky)

  implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WithSchema = new row.WithSchema(schema)
  implicit def extractor2Resolved[T, R](extractor: Alias[T, R]): GenResolved[T, R] = GenResolved(
    extractor.resolve(schema),
    TypedField(
      extractor.field,
      extractor.resolveType(schema)
    )
  )

  implicit def extractor2Function[T, R](extractor: GenExtractor[T, R]): PartialFunction[T, R] = extractor.resolve(schema)

  lazy val driverFactory: DriverFactory[CleanWebDriver] = SpookyConf.TEST_WEBDRIVER_FACTORY

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      SpookyUtils.retry(4) { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  import com.tribbloids.spookystuff.utils.SpookyViews.SparkContextView

  override def beforeAll() {

    TestHelper.TestSparkConf.setAppName("test")

    super.beforeAll()
  }

  override def afterAll() {

    val spooky = this.spooky
    TestHelper.clearTempDir()
    SpookyEnvFixture.shouldBeClean(spooky)
    sc.foreachNode {
      SpookyEnvFixture.shouldBeClean(spooky)
    }
    super.afterAll()
  }

  before{
    // bypass java.lang.NullPointerException at org.apache.spark.broadcast.TorrentBroadcast$.unpersist(TorrentBroadcast.scala:228)
    // TODO: clean up after fix
    SpookyUtils.retry(50) {
      setUp()
    }
  }

  after{
    tearDown()
  }

  def setUp(): Unit = {
//    SpookyEnvFixture.cleanDriverInstances()
    spooky.conf = new SpookyConf(
      autoSave = true,
      cacheWrite = false,
      cacheRead = false,
      components = Map(
        "dirs" -> new DirConf(
          root = SpookyUtils.\\\(TestHelper.TEMP_PATH, "spooky-unit")
        )
      )
    )

  }

  def tearDown(): Unit = {
    val spooky = this.spooky
    SpookyEnvFixture.driverInstancesShouldBeClean(spooky)
    sc.foreachNode {
      SpookyEnvFixture.driverInstancesShouldBeClean(spooky)
    }
  }
}