package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

/**
  * Created by peng on 02/04/16.
  */
class TestExtractPlan extends SpookyEnvSuite {

  import dsl._

  lazy val df = sql.createDataFrame(Seq(1 -> "a", 2 -> "b"))
  lazy val src = spooky.create(df)

  test("ExtractPlan.toString should work") {

    val extracted = src
      .extract{
        '_1.typed[Int].andFlatMap{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~! '_2
      }

    println(extracted.plan.toString)
  }

  test("ExtractPlan can overwrite old values using ~! operator") {
    val extracted = src
      .extract{
        '_1.typed[Int].andFlatMap{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~! '_2
      }
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "2")))
  }

  test("ExtractPlan can append on old values using ~+ operator") {
    val extracted = src
      .extract{
        '_1.typed[Int].andFlatMap{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~+ '_2
      }
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> Seq("a")), Map("_1" -> 2, "_2" -> Seq("b","2"))))
  }

  test("In ExtractPlan, weak values are cleaned in case of a conflict") {
    val extracted = src
      .extract{
        '_2 ~ '_3.*
      }
      .extract{
        '_1.typed[Int].andFlatMap{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~ '_3.*
      }
      .extract(
        '_3 ~ '_3 //force output
      )
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "b", "_3" -> "2")))
  }

  test("In ExtractPlan, weak values are not cleaned if being overwritten using ~! operator") {
    val extracted = src
      .extract{
        '_2 ~ '_3.*
      }
      .extract{
        '_1.typed[Int].andFlatMap{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~! '_3.*
      }
      .extract(
        '_3 ~ '_3
      )
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> "a", "_3" -> "a"), Map("_1" -> 2, "_2" -> "b", "_3" -> "2")))
  }
}