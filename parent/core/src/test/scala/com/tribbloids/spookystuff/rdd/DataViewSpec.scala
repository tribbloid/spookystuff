package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.debug.print_@
import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.actions.{NoOp, Wget}
import com.tribbloids.spookystuff.execution.{ExplorePlan, FetchPlan, FlatMapPlan}
import com.tribbloids.spookystuff.metrics.Acc
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

import java.io.File
import scala.reflect.ClassTag

object DataViewSpec {

  val resource: FileDocsFixture.type = FileDocsFixture
}

/**
  * Created by peng on 5/10/15.
  */
class DataViewSpec extends SpookyBaseSpec {

  import DataViewSpec.resource.*

  describe("should not run preceding transformation twice+") {

    it(s"map") {
      val acc = Acc.create(0)

      val ds = spooky
        .fetch(_ => Wget(HTML_URL))
        .map { _ =>
          acc += 1
          ()
        }
      assert(acc.value == 0)

      ds.count()
      assert(acc.value == 1)
    }

    it(s"squashedRDD") {
      val acc = Acc.create(0)

      val ds = spooky
        .fetch(_ => Wget(HTML_URL))
        .select { _ =>
          acc += 1
          ()
        }
      assert(acc.value == 0)

      val rdd = ds.squashedRDD
      assert(acc.value == 0)

      rdd.count()
      assert(acc.value == 1)
    }

    Seq(
      "unsorted" -> false,
      "sorted" -> true
    )
      .foreach {
        case (group, sorted) =>

          def render[D: ClassTag: Ordering](ds: DataView[D]): DataView[D] = {

            if (sorted) ds.sorted()
            else ds
          }

          describe(group) {

            it(s"toRDD") {
              val acc = Acc.create(0)

              val ds = spooky
                .fetch(_ => Wget(HTML_URL))
                .selectMany { row =>
                  print_@("hit acc")
                  acc += 1
                  row.docs.flatMap(_.saved.headOption)
                }
              assert(acc.value == 0)

              val rdd = render(ds).rdd

              rdd.count()
              assert(acc.value == 1)
            }

            it(s"toDF") {
              val acc = Acc.create(0)

              val ds = spooky
                .fetch(_ => Wget(HTML_URL))
                .selectMany { row =>
                  acc += 1
                  row.docs.flatMap(_.saved.headOption)

                }
              assert(acc.value == 0)

              val df = (DataView.TypedDatasetView(render(ds)): DataView.TypedDatasetView[String]).toDF

              df.count()
              assert(acc.value == 1)
            }

            it(s"toJSON") {
              val acc = Acc.create(0)

              val ds = spooky
                .fetch(_ => Wget(HTML_URL))
                .selectMany { row =>
                  acc += 1
                  row.docs.flatMap(_.saved.headOption)
                }

              assert(acc.value == 0)

              val json = render(ds).toJSON
              //      assert(acc.value == 0)

              json.count()
              assert(acc.value == 1)
            }
          }
      }
  }

  // if not adding up to 1, this is the debugging method:
  // 1. add breakpoint on accumulator, execute to it >1 times and dump a memory snapshot
  // 2. compare stacktrace of executor thread on both snapshots

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

  describe("persist on") {

    it(classOf[FetchPlan[?, ?]].getSimpleName) {
      val ds = spooky
        .fetch(_ => Wget(HTML_URL))
        .persist()
      ds.count()

      assert(ds.ctx.spookyMetrics.pagesFetched.value == 1)

      ds.fetch(_ => Wget(JSON_URL)).count()

      assert(ds.ctx.spookyMetrics.pagesFetched.value == 2)
    }

    it(classOf[FlatMapPlan[?, ?]].getSimpleName) {
      val ds = spooky
        .fetch(_ => Wget(HTML_URL))
        .select(_ => "Wikipedia")
        .persist()
      ds.count()

      assert(ds.ctx.spookyMetrics.pagesFetched.value == 1)

      ds.fetch(_ => Wget(JSON_URL)).count()

      assert(ds.ctx.spookyMetrics.pagesFetched.value == 2)
    }

    it(classOf[ExplorePlan[?, ?]].getSimpleName) {
      val entry: DataView[Unit] = spooky
        .fetch { _ =>
          Wget(DEEP_DIR_URL)
        }
      val ds = entry
        .recursively()
        .explore { row =>
          val docs = row.docs

          val dirs = docs.\("root directory")

          val path = dirs.\("path").text

          val result = path match {
            case Some(p) => Wget(p)
            case None    => NoOp
          }

          result
        }
        .persist()
      assert(ds.count() == 4)
      assert(ds.ctx.spookyMetrics.pagesFetched.value == 4)

      assert(ds.fetch(_ => Wget(JSON_URL)).count() == 4)
      assert(ds.ctx.spookyMetrics.pagesFetched.value == 5) // locality group optimiser kick in
    }
  }

  describe("savePage") {

    def dummyFileExists() = {
      val file = new File(Envs.USER_DIR \\ "temp" \\ "dummy.html")
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
