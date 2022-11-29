package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.web.conf.Web
import com.tribbloids.spookystuff.ActionException
import com.tribbloids.spookystuff.testutils.SpookyEnvFixture
import org.apache.spark.rdd.RDD

import scala.concurrent.duration

class WebActionSuite extends SpookyEnvFixture {

  import WebActionSuite._

  import duration._

  val exampleActionList: List[Action] = List(
    Click("dummy"),
    Wget("'{~}").as('dummy_name),
    ClusterRetry(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )
  )

  // TODO: finish assertion
  exampleActionList.foreach { a =>
    it(s"${a.getClass.getSimpleName} has an UDT") {
      val rdd: RDD[(Selector, Action)] = sc.parallelize(Seq(("1": Selector) -> a))
      val df = sql.createDataFrame(rdd)

      df.show(false)
      df.printSchema()

      //        df.toJSON.collect().foreach(println)
    }
  }

  describe("Click") {
    val action = Click("o1")

    it("-> JSON") {
      val str = action.prettyJSON() // TODO: add as a trait
      str.shouldBe(
        """
          |{
          |  "selector" : "By.sizzleCssSelector: o1",
          |  "cooldown" : "0 seconds",
          |  "blocking" : true
          |}
        """.stripMargin
      )
    }

    it("-> memberStrPretty") {
      val str = action.memberStrPretty // TODO: add as a trait

//      val codec: MessageWriter[_] = action

      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
          |Click(
          |  selector = By.sizzleCssSelector: o1,
          |  cooldown = 0 seconds,
          |  blocking = true
          |)
        """.stripMargin
        )
    }
  }

  describe("Loop") {
    val action = Loop(
      Click("o1")
        +> Snapshot()
    )

    it("-> JSON") {
      val str = action.prettyJSON()
      str.shouldBe(
        """
          |{
          |  "children" : [ {
          |    "selector" : "By.sizzleCssSelector: o1",
          |    "cooldown" : "0 seconds",
          |    "blocking" : true
          |  }, {
          |    "filter" : { }
          |  } ],
          |  "limit" : 2147483647
          |}
        """.stripMargin
      )
    }

    it("-> memberStrPretty") {
      val str = action.memberStrPretty
      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
          |Loop(
          |  children = List(
          |    Map(
          |      selector = By.sizzleCssSelector: o1,
          |      cooldown = 0 seconds,
          |      blocking = true
          |    ),
          |    Map(
          |      filter = MustHaveTitle
          |    )
          |  ),
          |  limit = 2147483647
          |)
        """.stripMargin
        )
    }
  }

  it("a session without webDriver initialized won't trigger errorDump") {
    try {
      DefectiveExport.fetch(spooky)
      sys.error("impossible")
    } catch {
      case e: ActionException =>
        assert(!e.getMessage.contains("Snapshot"))
        assert(!e.getMessage.contains("Screenshot"))
    }
  }

  it("a session with webDriver initialized will trigger errorDump, which should not be blocked by DocFilter") {
    try {
      DefectiveWebExport.fetch(spooky)
      sys.error("impossible")
    } catch {
      case e: ActionException =>
        println(e)
        assert(e.getMessage.contains("ErrorDump/DefectiveWebExport"))
        assert(e.getMessage.contains("ErrorScreenshot/DefectiveWebExport"))
    }
  }

  it("errorDump at the end of a series of actions should contains all backtraces") {
    try {
      (
        Delay(1.seconds) +>
          DefectiveWebExport
      ).fetch(spooky)
      sys.error("impossible")
    } catch {
      case e: ActionException =>
        println(e)
        assert(e.getMessage.contains("Delay/1_second/ErrorDump/DefectiveWebExport"))
        assert(e.getMessage.contains("Delay/1_second/ErrorScreenshot/DefectiveWebExport"))
    }
  }
}

object WebActionSuite {

  case object DefectiveExport extends Export {

    override def doExeNoName(session: Session): Seq[DocOption] = {
      sys.error("error")
    }
  }

  case object DefectiveWebExport extends Export with WebAction {

    override def doExeNoName(session: Session): Seq[DocOption] = {
      session.driverOf(Web)
      sys.error("error")
    }
  }
}
