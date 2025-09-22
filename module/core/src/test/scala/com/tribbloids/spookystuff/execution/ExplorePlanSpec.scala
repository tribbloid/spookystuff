package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl.Locality
import com.tribbloids.spookystuff.dsl.DataView._typedDatasetView
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer
import com.tribbloids.spookystuff.SpookyContext

/**
  * Created by peng on 05/04/16.
  */
class ExplorePlanSpec extends SpookyBaseSpec with FileDocsFixture {

  it("should create a new beaconRDD if its upstream doesn't have one") {
    val partitioner = new HashPartitioner(8)

    val src = spooky
      .select(_ => "abc")

    assert(src.plan.beaconRDDOpt.isEmpty)

    val d1 = src
      .recursively()
      .explore(
        _ => Wget(HTML_URL),
        locality = Locality.DocCacheAware { _ =>
          partitioner
        }
      )

    assert(d1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
  }

  it("should inherit old beaconRDD from upstream if exists") {
    val partitioner = new HashPartitioner(8)
    val partitioner2 = new HashPartitioner(16)

    val src = spooky
      .select(_ => "abc")

    val d1 = src
      .recursively()
      .explore(
        _ => Wget(HTML_URL),
        locality = Locality.DocCacheAware { _ =>
          partitioner
        }
      )

    assert(d1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    val beaconRDD = d1.plan.beaconRDDOpt.get

    val d2 = d1
      .recursively()
      .explore(
        _ => Wget(HTML_URL),
        locality = Locality.DocCacheAware { _ =>
          partitioner2
        }
      )

    assert(d2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    assert(d2.plan.beaconRDDOpt.get eq beaconRDD)
  }

  describe("should work on directory, range:") {

    val resourcePath = DEEP_DIR_URL

    def computeRange(range: Range, sorted: Boolean = true): (Seq[Row], SpookyContext) = {

      val dv = spooky
        .create(Seq(resourcePath))
        .fetch { row =>
          Wget(row.data)
        }
        .map { _ =>
          ArrayBuffer[Int]()
        }
        .recursively(range = range)
        .map { r =>
          val trace = r.data.raw
          trace += r.index

          val dirs = r.trajectory.docs.findAll("root directory").findAll("URI").texts

          (trace.toSeq, r.data.depth, dirs)
        }
        .explore { r =>
          val dirs = r.data.raw._3

          dirs.map { v =>
            Wget(v)
          }
        }
        .flatMap { r =>
          val files = r.trajectory.docs.findAll("root file").zipWithIndex

          files.map {
            case (elem, i) =>

              (r.data.raw, elem.findOnly("name").text.get, elem.findOnly("URI").text.get, i)
          }
        }

      val dv2 = if (sorted) dv.sortBy(r => r.data._1._2) else dv

      val df: DataFrame = dv2.asDataFrame

      val result: Seq[Row] = df.collect().toList
      result -> dv2.ctx
    }

    lazy val bigInt = Int.MaxValue - 10

    lazy val `0..` = computeRange(0 to bigInt)

    it("from 0") {

      val (result, ctx) = `0..`
      result
        .mkString("\n")
        .stripTmpRoot
        .shouldBe(
          """
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],Test.pdf,file:///testutils/dir/Test.pdf,0]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],Wikipedia.html,file:///testutils/dir/Wikipedia.html,1]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],example.xml,file:///testutils/dir/example.xml,2]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],hivetable.csv,file:///testutils/dir/hivetable.csv,3]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],logo11w.png,file:///testutils/dir/logo11w.png,4]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],table.csv,file:///testutils/dir/table.csv,5]
            |[[ArraySeq(0),0,ArraySeq(file:///testutils/dir/dir)],tribbloid.json,file:///testutils/dir/tribbloid.json,6]
            |[[ArraySeq(0, 0),1,ArraySeq(file:///testutils/dir/dir/dir)],Test.pdf,file:///testutils/dir/dir/Test.pdf,0]
            |[[ArraySeq(0, 0, 0),2,ArraySeq(file:///testutils/dir/dir/dir/dir)],pom.xml,file:///testutils/dir/dir/dir/pom.xml,0]
            |[[ArraySeq(0, 0, 0, 0),3,ArraySeq()],tribbloid.json,file:///testutils/dir/dir/dir/dir/tribbloid.json,0]
            |""".stripMargin
        )

      assert(ctx.metrics.pagesFetched.value == 4)
    }

    it(" ... unsorted") {

      val (result, ctx) = computeRange(0 to bigInt, false)

      result
        .mkString("\n")
        .shouldBe(
          `0..`._1.mkString("\n"),
          sort = true
        )

      assert(ctx.metrics.pagesFetched.value == 4)
    }

    it("0 to 2") {

      val (result, ctx) = computeRange(0 to 2)

      result
        .mkString("\n")
        .shouldBe(
          `0..`._1.slice(0, 8).mkString("\n")
        )
      assert(ctx.metrics.pagesFetched.value == 2)
    }

    it("from 2") {

      val (result, ctx) = computeRange(2 to bigInt)
      result
        .mkString("\n")
        .shouldBe(
          `0..`._1.slice(8, bigInt).mkString("\n")
        )
      assert(ctx.metrics.pagesFetched.value == 4)
    }

    it("2 to 2") {

      val (result, ctx) = computeRange(2 to 2)
      result
        .mkString("\n")
        .shouldBe(
          `0..`._1.slice(8, 8).mkString("\n")
        )
      assert(ctx.metrics.pagesFetched.value == 2)
    }

  }
}

object ExplorePlanSpec {

  //  object CustomKeyBy extends (Trace => Any) with Serializable {
  //
  //    override def apply(actions: Trace): Any = {
  //
  //      val uris = actions.collect {
  //        case v: Wget => v.uri.value
  //      }
  //      val parts = uris.head.split('/')
  //      val key = parts.slice(parts.length - 2, parts.length).toList
  //      key
  //    }
  //  }
}
