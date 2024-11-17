package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.metrics.Acc
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

import java.io.File
import scala.reflect.ClassTag

/**
  * Created by peng on 5/10/15.
  */
class SpookyDatasetSpec extends SpookyBaseSpec with FileDocsFixture {

  it(s".map should not run preceding transformation multiple times") {
    val acc = Acc.create(0)

    val set = spooky
      .fetch(_ => Wget(HTML_URL))
      .map { row =>
        acc += 1
        row.data
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
      .fetch(_ => Wget(HTML_URL))
      .select { _ =>
        acc += 1
        ()

        //            S.andFlatMap { page =>
        //              acc += 1
        //              page.saved.headOption
        //            } ~ 'path
      }
    assert(acc.value == 0)

    val rdd = set.squashedRDD
    assert(acc.value == 0)

    rdd.count()
    assert(acc.value == 1)
  }

  // if not adding up to 1, this is the debugging method:
  // 1. add breakpoint on accumulator, execute to it >1 times and dump a memory snapshot
  // 2. compare stacktrace of executor thread on both snapshots
  for (case (sort, comment) <- Seq(false -> "unsorted", true -> "sorted")) {

    def getData[D: ClassTag: Ordering](ds: SpookyDataset[D]): SpookyDataset.DataView[D] = {

      if (sort) ds.data.sorted()
      else ds.data
    }

    describe("should not run preceding transformation multiple times") {

      it(s"toRDD($comment)") {
        val acc = Acc.create(0)

        val ds = spooky
          .fetch(_ => Wget(HTML_URL))
          .selectMany { row =>
            acc += 1
            row.docs.flatMap(_.saved.headOption)
          }
        assert(acc.value == 0)

        val rdd = getData(ds).toRDD
        //      assert(acc.value == 0)

        rdd.count()
        assert(acc.value == 1)
      }

      it(s"toDF($comment)") {
        val acc = Acc.create(0)

        val ds = spooky
          .fetch(_ => Wget(HTML_URL))
          .selectMany { row =>
            acc += 1
            row.docs.flatMap(_.saved.headOption)

          }
        assert(acc.value == 0)

        val df = getData(ds).toDF

        df.count()
        assert(acc.value == 1)
      }

      it(s"toJSON($comment)") {
        val acc = Acc.create(0)

        val ds = spooky
          .fetch(_ => Wget(HTML_URL))
          .selectMany { row =>
            acc += 1
            row.docs.flatMap(_.saved.headOption)
          }

        assert(acc.value == 0)

        val json = getData(ds).toJSON
        //      assert(acc.value == 0)

        json.count()
        assert(acc.value == 1)
      }
    }

  }

//  // TODO: move to a new module
//  describe("toDF can") {
//
//    it("handle simple types") {
//
//      val set = spooky
//        .fetch(_ => Wget(HTML_URL))
//        .selectMany(
//          S.uri ~ 'uri,
//          S.children("h1").size ~ 'size,
//          S.timestamp ~ 'timestamp,
//          S.andFlatMap { page =>
//            page.saved.headOption
//          } ~ 'saved
//        )
//      val df = set.toDF()
//
//      df.schema.treeString.shouldBe(
//        """
//          |root
//          | |-- uri: string (nullable = true)
//          | |-- size: integer (nullable = true)
//          | |-- timestamp: timestamp (nullable = true)
//          | |-- saved: string (nullable = true)
//      """.stripMargin
//      )
//
//      df.show(false)
//    }
//
//    it("handle composite types") {
//      val set = spooky
//        .selectMany(
//          Lit(0 -> "str") ~ 'tuple,
//          Lit(Composite()) ~ 'composite
//        )
//
//      val df = set.toDF()
//
//      df.schema.treeString.shouldBe(
//        """
//          |root
//          | |-- tuple: struct (nullable = true)
//          | |    |-- _1: integer (nullable = false)
//          | |    |-- _2: string (nullable = true)
//          | |-- composite: struct (nullable = true)
//          | |    |-- n: integer (nullable = false)
//          | |    |-- str: string (nullable = true)
//      """.stripMargin
//      )
//
//      df.show(false)
//    }
//
//    it("yield a DataFrame excluding Fields with .isSelected = false") {
//
//      val set = spooky
//        .fetch(_ => Wget(HTML_URL))
//        .selectMany(
//          S.uri ~ 'uri.*,
//          S.children("h1").size ~ 'size.*,
//          S.timestamp ~ 'timestamp,
//          S.andFlatMap { page =>
//            page.saved.headOption
//          } ~ 'saved
//        )
//      val df = set.toDF()
//
//      df.schema.treeString.shouldBe(
//        """
//          |root
//          | |-- timestamp: timestamp (nullable = true)
//          | |-- saved: string (nullable = true)
//      """.stripMargin
//      )
//
//      df.show(false)
//    }
//  }

  it("fetch plan can be persisted") {
    val ds = spooky
      .fetch(_ => Wget(HTML_URL))
      .persist()
    ds.count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
    ds.spooky.spookyMetrics.resetAll()

    ds.fetch(_ => Wget(JSON_URL)).count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  it("selectMany plan can be persisted") {
    val ds = spooky
      .fetch(_ => Wget(HTML_URL))
      .select(_ => "Wikipedia")
      .persist()
    ds.count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
    ds.spooky.spookyMetrics.resetAll()

    ds.fetch(_ => Wget(JSON_URL))
      .count()

    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
  }

  // TODO: move to new submodule
//  it("flatten plan can be persisted") {
//    val ds = spooky
//      .fetch(_ => Wget(HTML_URL))
//      .explode(
//        Lit(Array("a" -> 1, "b" -> 2)) ~ 'Array
//      )
//      .persist()
//    ds.count()
//
//    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
//    ds.spooky.spookyMetrics.resetAll()
//
//    ds.wget(
//      JSON_URL
//    ).count()
//
//    assert(ds.spooky.spookyMetrics.pagesFetched.value == 1)
//  }

  it("explore plan can be persisted") {
    val first: SpookyDataset[Unit] = spooky
      .fetch { _ =>
        Wget(DEEP_DIR_URL)
      }
    val ds = first
      .inductively()
      .fetch { row =>
        val path: String = row.docs.\("root directory").attr("path").get

        Wget(path)
      }
      .persist()
    ds.count()

    ds.spooky.spookyMetrics.resetAll()

    ds.fetch(_ => Wget(JSON_URL)).count()

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
        .fetch(_ => Wget(HTML_URL))
        .select { row =>
          row.docs.save(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)
        }
        .collect()

      assert(dummyFileExists())
    }

    it("... on persisted RDD") {

      val vv = spooky
        .fetch(_ => Wget(HTML_URL))
        .persist()

      vv.collect()

      vv.select { row =>
        row.docs.save(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)
      }.collect()

      assert(dummyFileExists())

    }

    it("eagerly") {

      spooky
        .fetch(_ => Wget(HTML_URL))
        .select { row =>
          row.docs.save(s"file://${Envs.USER_DIR}/temp/dummy", overwrite = true)
        }
        .execute()

      assert(dummyFileExists())
    }
  }
}
