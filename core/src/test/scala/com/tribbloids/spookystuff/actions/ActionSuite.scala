package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.{ActionException, Const, SpookyEnvFixture}
import org.apache.spark.ml.dsl.utils.messaging.MessageWriter
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.concurrent.{duration, TimeoutException}
import scala.util.Random

class ActionSuite extends SpookyEnvFixture {

  import duration._

  it("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Wget(Const.keyDelimiter + "{~}").in(randomTimeout)

    val rewritten = action
      .interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema)
      .get

    //    val a = rewritten.uri.asInstanceOf[Literal[FR, String]].dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val b = Literal("http://www.dummy.com").dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val c = Literal(new Example()).dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize

    assert(rewritten === Wget(Lit.erased("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  it("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action
      .interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema)
      .get

    assert(rewritten === Wget(Lit.erased("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  val exampleActionList: List[Action] = List(
    Click("dummy"),
    Wget("'{~}").as('dummy_name),
    ClusterRetry(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )
  )

  //TODO: finish assertion
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
      val str = action.prettyJSON() //TODO: add as a trait
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
      val str = action.memberStrPretty //TODO: add as a trait

      val codec: MessageWriter[_] = action

      str.shouldBe(
        """
          |Click(
          |	By.sizzleCssSelector: o1,
          |	0 seconds,
          |	true
          |)
        """.stripMargin
      )
    }
  }

  describe("Wget") {
    val action = Wget("http://dummy.com")

    it("-> JSON") {
      val str = action.prettyJSON()
      str.shouldBe(
        """
          |{
          |  "uri" : "http://dummy.com",
          |  "filter" : { }
          |}
        """.stripMargin
      )
    }

    it("-> memberStrPretty") {
      val str = action.memberStrPretty
      str.shouldBe(
        """
          |Wget(
          |	http://dummy.com,
          |	MustHaveTitle
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
      str.shouldBe(
        """
          |Loop(
          |	List(
          |		Click(
          |			By.sizzleCssSelector: o1,
          |			0 seconds,
          |			true
          |		),
          |		Snapshot(
          |			MustHaveTitle
          |		)
          |	),
          |	2147483647
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
      ).head
        .fetch(spooky)
      sys.error("impossible")
    } catch {
      case e: ActionException =>
        println(e)
        assert(e.getMessage.contains("Delay/1_second/ErrorDump/DefectiveWebExport"))
        assert(e.getMessage.contains("Delay/1_second/ErrorScreenshot/DefectiveWebExport"))
    }
  }

  it("Timed mixin can terminate execution if it takes too long") {

    val a = OverdueExport
    val session = new Session(this.spooky)
    assert(a.hardTerminateTimeout(session) == spookyConf.remoteResourceTimeout + Const.hardTerminateOverhead)
    a in 10.seconds
    assert(a.hardTerminateTimeout(session) == 10.seconds + Const.hardTerminateOverhead)

    val (result, time) = CommonUtils.timed {
      try {
        a.fetch(spooky)
        sys.error("impossible")
      } catch {
        case e: ActionException =>
          println(e)
          assert(e.getCause.isInstanceOf[TimeoutException])
      }
    }
    assert(time <= 40 * 1000 * Const.remoteResourceLocalRetries)
  }
}

case object DefectiveExport extends Export {

  override def doExeNoName(session: Session): Seq[DocOption] = {
    sys.error("error")
  }
}

case object DefectiveWebExport extends Export {

  override def doExeNoName(session: Session): Seq[DocOption] = {
    session.webDriver
    sys.error("error")
  }
}

case object OverdueExport extends Export with Timed {

  override def doExeNoName(session: Session): Seq[DocOption] = {
    Thread.sleep(120 * 1000)
    Nil
  }
}
