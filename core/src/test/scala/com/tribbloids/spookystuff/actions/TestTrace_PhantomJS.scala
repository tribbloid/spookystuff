package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.session.{AbstractSession, CleanWebDriver, Session}
import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.xmlbeans.impl.common.XPath.ExecutionContext

class TestTrace_PhantomJS extends SpookyEnvFixture with FunSpecx {

  import com.tribbloids.spookystuff.dsl._
  import scala.concurrent.duration._

  override lazy val driverFactory: DriverFactory[CleanWebDriver] = DriverFactories.PhantomJS()

  it("inject output names should change output doc names") {

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

  it("dryrun should discard preceding actions when calculating Driverless action's backtrace") {

    val dry = (Delay(10.seconds) +> Wget("http://dum.my")).head.dryrun
    assert(dry.size == 1)
    assert(dry.head == Seq(Wget("http://dum.my")))

    val dry2 = (Delay(10.seconds) +> OAuthV2(Wget("http://dum.my"))).head.dryrun
    assert(dry2.size == 1)
    assert(dry2.head == Seq(OAuthV2(Wget("http://dum.my"))))
  }

  it("TraceView.autoSnapshot should not modify empty Trace") {

    assert(TraceView(List[Action]()).rewrite(defaultEC) == List[Action]())
  }

  it("TraceView.autoSnapshot should append Snapshot to non-empty Trace that doesn't end with Export OR Block") {

    val trace = List(
      Visit("dummy"),
      Snapshot() ~ 'A,
      Click("dummy")
    )

    assert(trace.rewrite(defaultEC) == trace :+ Snapshot())
  }

  it("TraceView.autoSnapshot should append Snapshot to non-empty Trace that has no output") {

    val trace = List(
      Visit("dummy"),
      Snapshot() ~ 'A,
      Loop(
        TextInput("dummy", 'dummy) +>
          Click("dummy")
      )
    )

    assert(trace.rewrite(defaultEC) == trace :+ Snapshot())
  }

  it("TraceView.TreeNode.toString should have indentations of TreeNode") {
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

  it("visit and snapshot") {
    val builder = new Session(spooky)
    Visit("http://www.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Doc]

    //    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))

    assert(page.uri contains "//www.wikipedia.org/")
  }

  it("visit, input submit and snapshot") {
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

  it("sizzle selector should work") {

    val results = (
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
      ).fetch(spooky)

    val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
    assert(code.contains("Wikipedia"))
  }

  //TODO: put in IT?
  it("visit should handle corsera") {
    val results = (
      Visit("https://www.coursera.org/yale") ::
        Snapshot() :: Nil
      )
      .fetch(spooky)

    val title = results.head.asInstanceOf[Doc]
      .root.\("title").head.text.get
    assert(title.toLowerCase.contains("coursera"))
  }

  // This is fundamentally conflicting with session & driver management
//  ignore("TraceView.apply should yield lazy stream") {
//
//    var acc: Int = 0
//
//    case object DummyAction extends Action {
//
//      override def outputNames: Set[String] = Set("dummy")
//
//      override protected def doExe(session: Session): Seq[Fetched] = {
//        acc += 1
//        Seq(NoDoc(Nil))
//      }
//    }
//
//    val actions = List(
//      DummyAction,
//      DummyAction,
//      DummyAction
//    )
//
//    spooky.withSession {
//      session =>
//        val results = actions.apply(session)
//        assert(acc == 1) // preemptive execution
//
//        results.headOption
//        assert(acc == 1)
//
//        results.toList
//        assert(acc == 3)
//
//        // in comparison
////        acc = 0
////        val notLazy = TraceView(actions)._apply(session, lazyStream = false)
////        assert(acc == 3)
//    }
//  }
}
