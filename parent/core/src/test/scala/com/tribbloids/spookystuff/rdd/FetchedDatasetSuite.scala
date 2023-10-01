package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.metrics.Acc
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyEnvFixture}
import com.tribbloids.spookystuff.testutils.beans.Composite
import com.tribbloids.spookystuff.dsl

/**
  * Created by peng on 5/10/15.
  */
class FetchedDatasetSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  import dsl._

  //    test("should support repartition") {
  //      val spooky = this.spooky
  //
  //      sc.setCheckpointDir(s"file://${CommonConst.USER_DIR}/temp/spooky-unit/${this.getClass.getSimpleName}/")
  //
  //      val first = spooky
  //        .fetch(Wget("http://en.wikipedia.org")).persist()
  //      first.checkpoint()
  //      first.count()
  //
  //      val second = first.wgetJoin(S"a".hrefs, joinType = LeftOuter)
  //        .extract(S.uri ~ 'uri)
  //        .repartition(14)
  //
  //      val result = second.collect()
  //      result.foreach(println)
  //
  //      assert(result.length == 2)
  //      assert(first.spooky.metrics.pagesFetched.value == 2)
  //    }

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
        S.andOptionFn { page =>
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
          S.andOptionFn { page =>
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
          S.andOptionFn { page =>
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
        .select(
          S.andOptionFn { page =>
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

  it("toDF can handle simple types") {

    val set = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .select(
        S.uri ~ 'uri,
        S.children("h1").size ~ 'size,
        S.timestamp ~ 'timestamp,
        S.andOptionFn { page =>
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

  it("toDF can handle composite types") {
    val set = spooky
      .select(
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

  it("toDF can yield a DataFrame excluding Fields with .isSelected = false") {

    val set = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .select(
        S.uri ~ 'uri.*,
        S.children("h1").size ~ 'size.*,
        S.timestamp ~ 'timestamp,
        S.andOptionFn { page =>
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
      .select(Lit("Wikipedia") ~ 'name)
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
      .flatten(
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
      )()
      .persist()
    ds.count()

    ds.spooky.spookyMetrics.resetAll()

    ds.wget(
      JSON_URL
    ).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  describe("savePage") {

    it("lazily") {

      spooky
        .fetch(
          Wget(HTML_URL)
        )
        .savePages("file://${CommonConst.USER_DIR}/temp/yyy", overwrite = true)
        .collect()
    }

    it("eagerly") {

      spooky
        .fetch(
          Wget(HTML_URL)
        )
        .savePages_!("file://${CommonConst.USER_DIR}/temp/yyy", overwrite = true)

    }
  }
}
