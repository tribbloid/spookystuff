package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.{dsl, SpookyEnvFixture}

/**
  * Created by peng on 02/04/16.
  */
class TestExtractPlan extends SpookyEnvFixture {

  import dsl._

  lazy val df = sql.createDataFrame(Seq(1 -> "a", 2 -> "b"))
  lazy val src = spooky.create(df)

  it("ExtractPlan can assign aliases to unnamed fields") {

    val extracted = src
      .extract(
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some("" + v)
          else None
        },
        '_2.typed[String].andOptionFn { v =>
          if (v.length < 5) Some(v.charAt(0).toInt)
          else None
        }
      )
      .persist()

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: string (nullable = true)
        | |-- _c1: string (nullable = true)
        | |-- _c2: integer (nullable = true)
      """.stripMargin
    )

    assert(
      extracted.toMapRDD().collect().toSeq ==
        Seq(Map("_1" -> 1, "_2" -> "a", "_c2" -> 97), Map("_1" -> 2, "_2" -> "b", "_c1" -> "2", "_c2" -> 98))
    )

    extracted.toDF().show(false)
  }

  it("ExtractPlan can overwrite old values using ! postfix") {

    val extracted = src
      .extract {
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some("" + v)
          else None
        } ~ '_2.!
      }

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: string (nullable = true)
      """.stripMargin
    )

    assert(
      extracted.toMapRDD().collect().toSeq ==
        Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "2"))
    )

    extracted.toDF().show(false)
  }

  it("ExtractPlan cannot partially overwrite old values with the same field id but different DataType") {

    intercept[IllegalArgumentException] {
      val extracted = src
        .extract {
          '_1.typed[Int].andOptionFn { v =>
            if (v > 1) Some(v)
            else None
          } ~ '_2.!
        }
    }
  }

  it("ExtractPlan can append to old values using ~+ operator") {
    val extracted = src
      .extract {
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some("" + v)
          else None
        } ~+ '_2
      }

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: array (nullable = true)
        | |    |-- element: string (containsNull = true)
      """.stripMargin
    )

    assert(
      extracted.toMapRDD().collect().toSeq == Seq(
        Map("_1" -> 1, "_2" -> Seq("a")),
        Map("_1" -> 2, "_2" -> Seq("b", "2"))
      )
    )

    extracted.toDF().show(false)
  }

  it("ExtractPlan can erase old values that has a different DataType using ~+ operator") {
    val extracted = src
      .extract {
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some(v)
          else None
        } ~+ '_2
      }

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
      """.stripMargin
    )

    assert(extracted.toMapRDD().collect().toSeq == Seq(Map("_1" -> 1, "_2" -> Seq()), Map("_1" -> 2, "_2" -> Seq(2))))

    extracted.toDF().show(false)
  }

  it("In ExtractPlan, weak values are cleaned in case of a conflict") {
    val extracted = src
      .extract {
        '_2 ~ '_3.*
      }
      .extract {
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some("" + v)
          else None
        } ~ '_3.*
      }
      .extract(
        '_3 ~ '_3 // force output
      )

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: string (nullable = true)
        | |-- _3: string (nullable = true)
        | |-- _3: string (nullable = true)
      """.stripMargin
    )

    assert(
      extracted.toMapRDD().collect().toSeq == Seq(Map("_1" -> 1, "_2" -> "a"), Map("_1" -> 2, "_2" -> "b", "_3" -> "2"))
    )

    extracted.toDF().show(false)
  }

  it("In ExtractPlan, weak values are not cleaned if being overwritten using ~! operator") {
    val extracted = src
      .extract {
        '_2 ~ '_3.*
      }
      .extract {
        '_1.typed[Int].andOptionFn { v =>
          if (v > 1) Some("" + v)
          else None
        } ~ '_3.*.!
      }
      .extract(
        '_3 ~ '_3
      )

    extracted.schema.structType.treeString.shouldBe(
      """
        |root
        | |-- _1: integer (nullable = true)
        | |-- _2: string (nullable = true)
        | |-- _3: string (nullable = true)
        | |-- _3: string (nullable = true)
      """.stripMargin
    )

    assert(
      extracted.toMapRDD().collect().toSeq == Seq(
        Map("_1" -> 1, "_2" -> "a", "_3" -> "a"),
        Map("_1" -> 2, "_2" -> "b", "_3" -> "2")
      )
    )

    extracted.toDF().show(false)
  }
}
