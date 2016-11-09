package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.{ActionException, Const, SpookyEnvFixture}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scala.util.Random

class TestAction extends SpookyEnvFixture {

  import duration._

  test("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Wget(Const.keyDelimiter+"{~}").in(randomTimeout)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), schema).get

//    val a = rewritten.uri.asInstanceOf[Literal[FR, String]].dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
//    val b = Literal("http://www.dummy.com").dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
//    val c = Literal(new Example()).dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize

    assert(rewritten === Wget(Literal.erase("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  test("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), schema).get

    assert(rewritten === Wget(Literal.erase("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  val exampleActionList: List[Action] = List(
    Click("dummy"),
    Wget("'{~}").as('dummy_name),
    Try(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )
  )

  //TODO: finish assertion
  exampleActionList.foreach{
    a =>
      test(s"${a.getClass.getSimpleName} has an UDT") {
        val rdd: RDD[(Selector, Action)] = sc.parallelize(Seq("1" -> a))
        val df = sql.createDataFrame(rdd)

        df.show(false)
        df.printSchema()

//        df.toJSON.collect().foreach(println)
      }
  }

  test("a session without webDriver initialized won't trigger errorDump") {
    try {
      ErrorExport.fetch(spooky)
      sys.error("impossible")
    }
    catch {
      case e: ActionException =>
        assert(!e.getMessage.contains("Snapshot"))
        assert(!e.getMessage.contains("Screenshot"))
    }
  }

  test("a session with webDriver initialized will trigger errorDump, which should not be blocked by DocFilter") {
    try {
      ErrorWebExport.fetch(spooky)
      sys.error("impossible")
    }
    catch {
      case e: ActionException =>
        assert(e.getMessage.contains("ErrorWebExport"))
        assert(e.getMessage.contains("Snapshot"))
        assert(e.getMessage.contains("Screenshot"))
    }
  }

//  test("a session with webDriver initialized will trigger errorDump, which should not be blocked by DocFilter") {
//    try {
//      ErrorWebExport.fetch(spooky)
//      sys.error("impossible")
//    }
//    catch {
//      case e: ActionException =>
//        assert(e.getMessage.contains("Snapshot"))
//        assert(e.getMessage.contains("Screenshot"))
//    }
//  }
}

case object ErrorExport extends Export {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    sys.error("error")
  }
}

case object ErrorWebExport extends Export {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    session.webDriver
    sys.error("error")
  }
}