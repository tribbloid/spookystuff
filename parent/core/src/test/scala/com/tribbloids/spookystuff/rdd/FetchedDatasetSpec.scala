package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.metrics.Acc
import com.tribbloids.spookystuff.testutils.beans.Composite
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

import java.io.File

/**
  * Created by peng on 5/10/15.
  */
class FetchedDatasetSpec extends SpookyBaseSpec with FileDocsFixture {

  it(s".map should not run preceding transformation multiple times") {
    val acc = Acc.create(0)

    val set = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .map { v =>
        acc += 1
        v
      }
    assert(acc.value == 0)

    //    val rdd = set.rdd
    //    assert(acc.value == 0)

    set.count()
    assert(acc.value == 1)
  }

  it(s".rdd should not run preceding transformation multiple times") {
    val acc = Acc.create(0)

    val set = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .extract(
        S.andFlatMap { page =>
          acc += 1
          page.saved.headOption
        } ~ 'path
      )
    assert(acc.value == 0)

    val rdd = set.squashedRDD
    assert(acc.value == 0)

    rdd.count()
    assert(acc.value == 1)
  }

  // if not adding up to 1, this is the debugging method:
  // 1. add breakpoint on accumulator, execute to it >1 times and dump a memory snapshot
  // 2. compare stacktrace of executor thread on both snapshots
  for (sort <- Seq(false, true)) {
    it(s"toDF($sort) should not run preceding transformation multiple times") {
      val acc = Acc.create(0)

      val set = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .extract(
          S.andFlatMap { page =>
            acc += 1
            page.saved.headOption
          } ~ 'path
        )
      assert(acc.value == 0)

      val df = set.toDF(sort)
      //      assert(acc.value == 0)

      df.count()
      assert(acc.value == 1)
    }

    it(s"toJSON($sort) should not run preceding transformation multiple times") {
      val acc = Acc.create(0)

      val set = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .extract(
          S.andFlatMap { page =>
            acc += 1
            page.saved.headOption
          } ~ 'path
        )
      assert(acc.value == 0)

      val json = set.toJSON(sort)
      //      assert(acc.value == 0)

      json.count()
      assert(acc.value == 1)
    }

    it(s"toMapRDD($sort) should not run preceding transformation multiple times") {
      val acc = Acc.create(0)

      val set = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .extract(
          S.andFlatMap { page =>
            acc += 1
            page.saved.headOption
          } ~ 'path
        )
      assert(acc.value == 0)

      val rdd = set.toMapRDD(sort)
      //      assert(acc.value == 0)

      rdd.count()
      assert(acc.value == 1)
    }
  }

  describe("toDF can") {

    it("handle simple types") {

      val set = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .extract(
          S.uri ~ 'uri,
          S.children("h1").size ~ 'size,
          S.timestamp ~ 'timestamp,
          S.andFlatMap { page =>
            page.saved.headOption
          } ~ 'saved
        )
      val df = set.toDF()

      df.schema.treeString.shouldBe(
        """
          |root
          | |-- uri: string (nullable = true)
          | |-- size: integer (nullable = true)
          | |-- timestamp: timestamp (nullable = true)
          | |-- saved: string (nullable = true)
      """.stripMargin
      )

      df.show(false)
    }

    it("handle composite types") {
      val set = spooky
        .extract(
          Lit(0 -> "str") ~ 'tuple,
          Lit(Composite()) ~ 'composite
        )

      val df = set.toDF()

      df.schema.treeString.shouldBe(
        """
          |root
          | |-- tuple: struct (nullable = true)
          | |    |-- _1: integer (nullable = false)
          | |    |-- _2: string (nullable = true)
          | |-- composite: struct (nullable = true)
          | |    |-- n: integer (nullable = false)
          | |    |-- str: string (nullable = true)
      """.stripMargin
      )

      df.show(false)
    }

    it("yield a DataFrame excluding Fields with .isSelected = false") {

      val set = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .extract(
          S.uri ~ 'uri.*,
          S.children("h1").size ~ 'size.*,
          S.timestamp ~ 'timestamp,
          S.andFlatMap { page =>
            page.saved.headOption
          } ~ 'saved
        )
      val df = set.toDF()

      df.schema.treeString.shouldBe(
        """
          |root
          | |-- timestamp: timestamp (nullable = true)
          | |-- saved: string (nullable = true)
      """.stripMargin
      )

      df.show(false)
    }
  }

  it("fetch plan can be persisted") {
    val ds = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .persist()
    ds.count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
    ds.spooky.spookyMetrics.resetAll()

    ds.wget(
      JSON_URL
    ).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  it("extract plan can be persisted") {
    val ds = spooky
      .wget(
        HTML_URL
      )
      .extract(Lit("Wikipedia") ~ 'name)
      .persist()
    ds.count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
    ds.spooky.spookyMetrics.resetAll()

    ds.wget(
      JSON_URL
    ).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  it("flatten plan can be persisted") {
    val ds = spooky
      .wget(
        HTML_URL
      )
      .explode(
        Lit(Array("a" -> 1, "b" -> 2)) ~ 'Array
      )
      .persist()
    ds.count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
    ds.spooky.spookyMetrics.resetAll()

    ds.wget(
      JSON_URL
    ).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  it("explore plan can be persisted") {
    val first = spooky
      .wget {
        DEEP_DIR_URL
      }
    val ds = first
      .explore(S"root directory".attr("path"))(
        Wget('A)
      )
      .persist()
    ds.count()

    ds.spooky.spookyMetrics.resetAll()

    ds.wget(
      JSON_URL
    ).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  describe("savePage") {

    def dummyFileExists() = {
      val file = new File(Envs.USER_DIR :\ "temp" :\ "dummy.html")
      val exists_deleted = file.exists() -> file.delete()
      exists_deleted._1 && exists_deleted._2
    }

    it("lazily") {

      spooky
        .fetch(
          Wget(HTML_URL)
        )
        .savePages(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)
        .collect()

      assert(dummyFileExists())
    }

    it("... on persisted RDD") {

      val vv = spooky
        .fetch(
          Wget(HTML_URL)
        )
        .persist()

      vv.collect()

      vv.savePages(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)
        .collect()

      assert(dummyFileExists())

    }

    it("eagerly") {

      spooky
        .fetch(
          Wget(HTML_URL)
        )
        .savePages_!(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)

      assert(dummyFileExists())
    }
  }
}
