package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.{CleanWebDriver, Session, AbstractSession}
import com.tribbloids.spookystuff.testutils.FunSuitex

class TestTrace_PhantomJS extends SpookyEnvFixture with FunSuitex {

  import com.tribbloids.spookystuff.dsl._

  import scala.concurrent.duration._

  override lazy val driverFactory: DriverFactory[CleanWebDriver] = DriverFactories.PhantomJS()

  test("inject output names should change output doc names") {

    val t1 = (
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        :: Snapshot().as('a)
        :: Loop (
        ClickNext("button.btn","1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot() ~'b
      ):: Nil
      )

    val t2 = (
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        :: Snapshot().as('c)
        :: Loop (
        ClickNext("button.btn","1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot() ~'d
      ):: Nil
      )

    t1.injectFrom(t2.asInstanceOf[t1.type ])

    assert(t1.outputNames === Set("c","d"))
  }

  test("dryrun should discard preceding actions when calculating Driverless action's backtrace") {

    val dry = (Delay(10.seconds) +> Wget("http://dum.my")).head.dryrun
    assert(dry.size == 1)
    assert(dry.head == Seq(Wget("http://dum.my")))

    val dry2 = (Delay(10.seconds) +> OAuthV2(Wget("http://dum.my"))).head.dryrun
    assert(dry2.size == 1)
    assert(dry2.head == Seq(OAuthV2(Wget("http://dum.my"))))
  }

  test("Trace.correct should not modify empty Trace") {

    assert(TraceView(List[Action]()).autoSnapshot == List[Action]())
  }

  test("Trace.correct should append Snapshot to non-empty Trace that doesn't end with Export OR Block") {

    val trace = List(
      Visit("dummy"),
      Snapshot() ~ 'A,
      Click("dummy")
    )

    assert(trace.autoSnapshot == trace :+ Snapshot())
  }

  test("Trace.correct should append Snapshot to non-empty Trace that has no output") {

    val trace = List(
      Visit("dummy"),
      Snapshot() ~ 'A,
      Loop(
        TextInput("dummy", 'dummy) +>
          Click("dummy")
      )
    )

    assert(trace.autoSnapshot == trace :+ Snapshot())
  }

  test("TraceView.TreeNode.toString should have indentations of TreeNode") {
    import com.tribbloids.spookystuff.dsl._

    val traces: Set[Trace] = (
      Visit(HTML_URL)
        +> Click("dummy")
        +> Snapshot()
        +> Loop(
        Click("next")
          +> TextInput("box", "something")
          +> Snapshot()
          +> If(
          {(v: Doc, _: AbstractSession) => v.uri startsWith "http" },
          Click("o1")
            +> TextInput("box1", "something1")
            +> Snapshot(),
          Click("o2")
            +> TextInput("box2", "something2")
            +> Snapshot()
        )
      )
      )

    traces.foreach{
      trace =>
        val str = TraceView(trace).TreeNode.toString
        println(str)
        assert(str contains "\n")
    }
  }

  //  test("Click.toString should work") {
  //    val action = Click("o1")
  //    val json = action.toString()
  //    println(json)
  //  }
  //
  //  test("Wget.toString should work") {
  //    val action = Wget("http://dummy.com")
  //    val json = action.toString()
  //    println(json)
  //  }
  //
  //  test("Loop.toString should work") {
  //    val action = Loop(
  //      Click("o1")
  //        +> Snapshot()
  //    )
  //    val json = action.toString()
  //    println(json)
  //  }

  test("Click.toJSON should work") {
    val action = Click("o1")
    val json = action.toJSON()
    json.shouldBe(
      """
        |{
        |  "className" : "com.tribbloids.spookystuff.actions.Click",
        |  "params" : {
        |    "selector" : "o1",
        |    "delay" : "0 seconds",
        |    "blocking" : true
        |  }
        |}
      """.stripMargin
    )
  }

  test("Wget.toJSON should work") {
    val action = Wget("http://dummy.com")
    val json = action.toJSON()
    json.shouldBe(
      """
        |{
        |  "className" : "com.tribbloids.spookystuff.actions.Wget",
        |  "params" : {
        |    "uri" : "http://dummy.com",
        |    "filter" : { }
        |  }
        |}
      """.stripMargin
    )
  }

  test("Loop.toJSON should work") {
    val action = Loop(
      Click("o1")
        +> Snapshot()
    )
    val json = action.toJSON()
    json.shouldBe(
      """
        |{
        |  "className" : "com.tribbloids.spookystuff.actions.Loop",
        |  "params" : {
        |    "children" : [ {
        |      "className" : "com.tribbloids.spookystuff.actions.Click",
        |      "params" : {
        |        "selector" : "o1",
        |        "delay" : "0 seconds",
        |        "blocking" : true
        |      }
        |    }, {
        |      "className" : "com.tribbloids.spookystuff.actions.Snapshot",
        |      "params" : {
        |        "filter" : { }
        |      }
        |    } ],
        |    "limit" : 2147483647
        |  }
        |}
      """.stripMargin
    )
  }

  //TODO: enable these
  //  test("Action.toJSON should work") {
  //    val actions = Loop(
  //      Click("next")
  //        +> TextInput("box", "something")
  //        +> Snapshot()
  //        +> If(
  //        { (v: Doc, _: Session) => v.uri startsWith "http" },
  //        Click("o1")
  //          +> TextInput("box1", "something1")
  //          +> Snapshot(),
  //        Click("o2")
  //          +> TextInput("box2", "something2")
  //          +> Snapshot()
  //      ))
  //
  //    val jsons = actions.map(
  //      v =>
  //        v.toJSON
  //    )
  //
  //    jsons.foreach(println)
  //  }

  //  test("Trace has a Dataset Encoder") {
  //    val trace =(
  //      Visit(HTML_URL)
  //        +> Click("dummy")
  //        +> Snapshot()
  //        +> Loop(
  //        Click("next")
  //          +> TextInput("box", "something")
  //          +> Snapshot()
  //          +> If(
  //          {v: Doc => v.uri startsWith "http" },
  //          Click("o1")
  //            +> TextInput("box1", "something1")
  //            +> Snapshot(),
  //          Click("o2")
  //            +> TextInput("box2", "something2")
  //            +> Snapshot()
  //        ))
  //      )
  //
  //    val df = sql.read.json(sc.parallelize(trace.toSeq.map(_.toJSON)))
  //
  //    implicit val encoder = Encoders.kryo[TraceView]
  //
  //    val ds = df.as[TraceView]
  //
  //    ds.collect().foreach(println)
  //  }

  test("visit and snapshot") {
    val builder = new Session(spooky)
    Visit("http://www.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Doc]

    //    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))

    assert(page.uri contains "//www.wikipedia.org/")
  }

  test("visit, input submit and snapshot") {
    val results = (
      Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput").in(40.seconds) ::
        Snapshot().as('A) ::
        TextInput("input#searchInput","Deep learning") ::
        Submit("button.pure-button") ::
        Snapshot().as('B) :: Nil
      ).fetch(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList.head.asInstanceOf[Doc]
    val res2 = resultsList(1).asInstanceOf[Doc]

    val id1 = Visit("http://www.wikipedia.org") ::
      WaitFor("input#searchInput") ::
      Snapshot().as('C) :: Nil
    assert(res1.uid.backtrace === id1)
    assert(res1.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
    assert(res1.uri contains "//www.wikipedia.org")
    assert(res1.name === "A")

    val id2 = Visit("http://www.wikipedia.org") ::
      WaitFor("input#searchInput") ::
      TextInput("input#searchInput", "Deep learning") ::
      Submit("button.pure-button") ::
      Snapshot().as('D) :: Nil
    assert(res2.uid.backtrace === id2)
    assert(res2.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning"))
    assert(res2.uri contains "//en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("sizzle selector should work") {

    val results = (
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
      ).fetch(spooky)

    val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
    assert(code.contains("Wikipedia"))
  }

  //  test("visit should handle corsera") {

  //TODO: PhantomJS is broken on this: re-enable after its fixed or switching to alternative browser.

  //    val results = (
  //      Visit("https://www.coursera.org/yale") ::
  //        Snapshot() :: Nil
  //      ).resolve(spooky)
  //
  //    val code = results.head.asInstanceOf[Page].code.get.split('\n').map(_.trim).mkString
  //    assert(code.contains("<title>Yale University"))
  //  }
}
