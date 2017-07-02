package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.{ActionException, Const, SpookyEnvFixture}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.concurrent.{TimeoutException, duration}
import scala.util.Random

class TestAction extends SpookyEnvFixture {

  import duration._

  it("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Wget(Const.keyDelimiter+"{~}").in(randomTimeout)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema).get

    //    val a = rewritten.uri.asInstanceOf[Literal[FR, String]].dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val b = Literal("http://www.dummy.com").dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val c = Literal(new Example()).dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize

    assert(rewritten === Wget(Lit.erase("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  it("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema).get

    assert(rewritten === Wget(Lit.erase("http://www.dummy.com")))
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
  exampleActionList.foreach{
    a =>
      it(s"${a.getClass.getSimpleName} has an UDT") {
        val rdd: RDD[(Selector, Action)] = sc.parallelize(Seq("1" -> a))
        val df = sql.createDataFrame(rdd)

        df.show(false)
        df.printSchema()

        //        df.toJSON.collect().foreach(println)
      }
  }

  it("a session without webDriver initialized won't trigger errorDump") {
    try {
      DefectiveExport.fetch(spooky)
      sys.error("impossible")
    }
    catch {
      case e: ActionException =>
        assert(!e.getMessage.contains("Snapshot"))
        assert(!e.getMessage.contains("Screenshot"))
    }
  }

  it("a session with webDriver initialized will trigger errorDump, which should not be blocked by DocFilter") {
    try {
      DefectiveWebExport.fetch(spooky)
      sys.error("impossible")
    }
    catch {
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
        )
        .head
        .fetch(spooky)
      sys.error("impossible")
    }
    catch {
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

    val (result, time) = TestHelper.timer {
      try {
        a
          .fetch(spooky)
        sys.error("impossible")
      }
      catch {
        case e: ActionException =>
          println(e)
          assert(e.getCause.isInstanceOf[TimeoutException])
      }
    }
    assert(time <= 40*1000*Const.remoteResourceLocalRetries)
  }
}

case object DefectiveExport extends Export {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    sys.error("error")
  }
}

case object DefectiveWebExport extends Export {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    session.webDriver
    sys.error("error")
  }
}

case object OverdueExport extends Export with Timed {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    Thread.sleep(120*1000)
    Nil
  }
}