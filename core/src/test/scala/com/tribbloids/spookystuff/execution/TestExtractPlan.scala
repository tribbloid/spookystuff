package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

/**
  * Created by peng on 02/04/16.
  */
class TestExtractPlan extends SpookyEnvSuite {

  import dsl._

  lazy val df = sql.createDataFrame(Seq(1 -> "a", 2 -> "b"))
  lazy val src = spooky.create(df)

  test("ExtractPlan can assign aliases to unnamed fields") {

    val extracted = src
      .extract(
        '_1.typed[Int].andOptionFn{
          v =>
            if (v > 1) Some("" + v)
            else None
        },
        '_2.typed[String].andOptionFn{
          v =>
            if (v.length < 5) Some(v.charAt(0).toInt)
            else None
        }
      )

    println(extracted.plan.toString)

    assert(
      extracted.toMapRDD().collect().toSeq ==
        Seq(Map("_1" -> 1, "_2" -> "a", "_c2" -> 97), Map("_1" -> 2, "_2" -> "b", "_c1" -> "2", "_c2" -> 98))
    )
  }

  test("ExtractPlan.toString should work") {

    val extracted = src
      .extract{
        '_1.typed[Int].andOptionFn{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~ '_2.!
      }

    println(extracted.plan.toString)

    assert(
      extracted.toMapRDD().collect().toSeq ==
        Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "2"))
    )
  }

  test("ExtractPlan can overwrite old values using ~! operator") {
    val extracted = src
      .extract{
        '_1.typed[Int].andOptionFn{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~ '_2.!
      }
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "2")))
  }

  test("ExtractPlan can append on old values using ~+ operator") {
    val extracted = src
      .extract{
        '_1.typed[Int].andOptionFn{
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
    val set = src
      .extract{
        '_2 ~ '_3.*
      }
      .extract{
        '_1.typed[Int].andOptionFn{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~ '_3.*
      }
      .extract(
        '_3 ~ '_3 //force output
      )
    val extracted = set
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
        '_1.typed[Int].andOptionFn{
          v =>
            if (v > 1) Some("" + v)
            else None
        } ~ '_3.*.!
      }
      .extract(
        '_3 ~ '_3
      )
      .toMapRDD()
      .collect()

    assert(extracted.toSeq == Seq(Map("_1" -> 1, "_2" -> "a", "_3" -> "a"), Map("_1" -> 2, "_2" -> "b", "_3" -> "2")))
  }
}