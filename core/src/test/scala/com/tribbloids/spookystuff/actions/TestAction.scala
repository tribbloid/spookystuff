package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field}
import com.tribbloids.spookystuff.{Const, SpookyEnvSuite}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.concurrent.duration
import scala.util.Random

class TestAction extends SpookyEnvSuite {

  import duration._

  test("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Visit(Const.keyDelimiter+"{~}").in(randomTimeout)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), schema).get

    assert(rewritten === Visit(Literal("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  test("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), schema).get

    assert(rewritten === Wget(Literal("http://www.dummy.com")))
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
}