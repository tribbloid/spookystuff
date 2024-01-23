package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.web.conf.Web
import com.tribbloids.spookystuff.ActionException
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import org.apache.spark.rdd.RDD

import scala.concurrent.duration

class WebActionSuite extends SpookyBaseSpec {

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

    it("-> treeText") {
      val str = action.treeText // TODO: add as a trait

      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
          |Click
          |  "By.sizzleCssSelector: o1"
          |  0 seconds
          |  true
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

    it("-> treeText") {
      val str = action.treeText
      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
          |Loop
          |  List
          |    Click
          |      "By.sizzleCssSelector: o1"
          |      0 seconds
          |      true
          |    Snapshot
          |      MustHaveTitle()
          |  2147483647
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
        assert(e.getMessage.contains("Snapshot/Bypass()/DefectiveWebExport"))
        assert(e.getMessage.contains("Screenshot/Bypass()/DefectiveWebExport"))
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
        assert(e.getMessage.contains("Delay/1_second/Snapshot/Bypass()/DefectiveWebExport"))
        assert(e.getMessage.contains("Delay/1_second/Screenshot/Bypass()/DefectiveWebExport"))
    }
  }
}

object WebActionSuite {

  case object DefectiveExport extends Export {

    override def doExeNoName(session: Session): Seq[Observation] = {
      sys.error("error")
    }
  }

  case object DefectiveWebExport extends Export with WebAction {

    override def doExeNoName(session: Session): Seq[Observation] = {
      session.driverOf(Web)
      sys.error("error")
    }
  }
}
