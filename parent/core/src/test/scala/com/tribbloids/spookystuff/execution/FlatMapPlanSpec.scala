package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.dsl.DataView
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 02/04/16.
  */
class FlatMapPlanSpec extends SpookyBaseSpec {

  import sql.implicits.*

  lazy val df = sql.createDataset(Seq(1 -> "a", 2 -> "b"))
  lazy val src = spooky.create(df)

  it("flatMap/selectMany") {

    val extracted = src
      .flatMap { row =>
        val v1 = Seq(row.data._1).flatMap { i =>
          0 to i
        }
        v1
      }
      .persist()

    extracted: DataView[Int]

    assert(
      extracted.dataRDD.collect().toSeq ==
        Seq(0, 1, 0, 1, 2)
    )
  }

  it("map/select") {

    val extracted = src
      .map { row =>
        val v1 = Seq(row.data._1).flatMap { i =>
          0 to i
        }
        v1
      }
      .persist()

    extracted: DataView[Seq[Int]]

    assert(
      extracted.dataRDD.collect().toSeq ==
        Seq(Seq(0, 1), Seq(0, 1, 2))
    )
  }
}
