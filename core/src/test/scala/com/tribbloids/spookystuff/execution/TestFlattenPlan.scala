package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyEnvSuite

/**
  * Created by peng on 17/05/16.
  */
class TestFlattenPlan extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  lazy val df = sql.createDataFrame(Seq(Seq(1, 2, 3) -> null, Seq(4, 5, 6) -> Seq("b", "c", "d")))
  lazy val src = spooky.create(df)

  test("FlattenPlan should work on collection") {
    val rdd1 = src
      .flatten('_1 ~ 'B)
      .toMapRDD(true)

    rdd1.collect().mkString("\n").shouldBe()
  }

  test("FlattenPlan should work on collection if overwriting defaultJoinField") {
    val rdd1 = src
      .flatten('_1 ~ 'A)
      .toMapRDD(true)

    rdd1.collect().mkString("\n").shouldBe()
  }

  test("FlattenPlan should work on partial collection") {
    val rdd1 = src
      .flatten('_2 ~ 'A)
      .toMapRDD(true)

    rdd1.collect().mkString("\n").shouldBe()
  }

  test("flatExtract is equivalent to flatten + extract") {
    val rdd1 = src
      .flatExtract('_2 ~ 'A)(
        'A ~ 'dummy
      )
      .toMapRDD(true)

    val rdd2 = src
      .flatten('_2 ~ 'A)
      .extract(
        'A ~ 'dummy
      )
      .toMapRDD(true)

    rdd1.collect().mkString("\n").shouldBe(
      rdd2.collect().mkString("\n")
    )
  }

  test("flatExtract is equivalent to flatten + extract if not manually set join key") {
    val rdd1 = src
      .flatExtract('_2 ~ 'A)(
        'A ~ 'dummy
      )
      .toMapRDD(true)

    val rdd2 = src
      .flatten('_2 ~ 'A)
      .extract(
        'A ~ 'dummy
      )
      .toMapRDD(true)

    rdd1.collect().mkString("\n").shouldBe(
      rdd2.collect().mkString("\n")
    )
  }
}
