package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Action, Wget}
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.{dsl, QueryException}
import org.apache.spark.HashPartitioner

/**
  * Created by peng on 05/04/16.
  */
class ExplorePlanSpec extends SpookyBaseSpec with LocalPathDocsFixture {

  import dsl._

  ignore("toString") { // de-prioritised

    val base = spooky
      .fetch(
        Wget("dummy")
      )

    val explored = base
      .explore(S"div.sidebar-nav a", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'depth
      )
      .extract(
        'A.text ~ 'category,
        S"h1".text ~ 'header
      )

    println(explored.plan.toString)
  }

  it("should create a new beaconRDD if its upstream doesn't have one") {
    val partitioner = new HashPartitioner(8)

    val src = spooky
      .extract(Lit("abc") ~ 'dummy)

    assert(src.plan.beaconRDDOpt.isEmpty)

    val rdd1 = src
      .explore('dummy)(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner)
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
  }

  it("should inherit old beaconRDD from upstream if exists") {
    val partitioner = new HashPartitioner(8)
    val partitioner2 = new HashPartitioner(16)

    val rdd1 = spooky
      .extract(Lit("abc") ~ 'dummy)
      .explore('dummy)(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner)
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    val beaconRDD = rdd1.plan.beaconRDDOpt.get

    val rdd2 = rdd1
      .explore('dummy)(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner2)
      )

    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
  }

  describe("should work on directory, with range starting from") {

    val resourcePath = DEEP_DIR_URL

    def computeFrom(range: Range) = {

      val df = spooky
        .create(Seq(resourcePath))
        .fetch {
          Wget('_)
        }
        .explore(S"root directory URI".text)(
          Wget('A),
          depthField = 'depth,
          range = range
        )
        .fork(S"root file")(
          A"name".text into 'leaf,
          A"URI".text ~ 'fullPath
        )
        .toDF(sort = true)

      val result = df.collect().toList
      result
    }

    lazy val computeFromMinus1 = computeFrom(-1 to 10)

    it("-1") {

      computeFromMinus1
        .mkString("\n")
        .shouldBe(
          """
            |[/tmp/spookystuff/resources/testutils/dir,null,null,null]
            |[/tmp/spookystuff/resources/testutils/dir,0,ArraySeq(hivetable.csv),file:///tmp/spookystuff/resources/testutils/dir/hivetable.csv]
            |[/tmp/spookystuff/resources/testutils/dir,0,ArraySeq(table.csv),file:///tmp/spookystuff/resources/testutils/dir/table.csv]
            |[/tmp/spookystuff/resources/testutils/dir,1,ArraySeq(hivetable.csv, Test.pdf),file:///tmp/spookystuff/resources/testutils/dir/dir/Test.pdf]
            |[/tmp/spookystuff/resources/testutils/dir,2,ArraySeq(hivetable.csv, Test.pdf, pom.xml),file:///tmp/spookystuff/resources/testutils/dir/dir/dir/pom.xml]
            |[/tmp/spookystuff/resources/testutils/dir,3,ArraySeq(hivetable.csv, Test.pdf, pom.xml, tribbloid.json),file:///tmp/spookystuff/resources/testutils/dir/dir/dir/dir/tribbloid.json]
            |""".stripMargin
        )
    }

    it("0") {

      computeFrom(0 to 10)
        .mkString("\n")
        .shouldBe(
          computeFromMinus1.drop(1).mkString("\n")
        )
    }

    it("0 to 2") {

      computeFrom(0 to 2)
        .mkString("\n")
        .shouldBe(
          computeFromMinus1.slice(1, 4).mkString("\n")
        )
    }

  }

  it("should throw an exception if OrdinalField == DepthField") {
    val rdd1 = spooky
      .fetch {
        Wget(HTML_URL)
      }

    intercept[QueryException] {
      rdd1
        .explore(S"root directory".attr("path"), ordinalField = 'dummy)(
          Wget('A),
          depthField = 'dummy
        )
    }
  }

  it("should avoid shuffling the latest batch to minimize repeated fetch") {
    val first = spooky
      .wget {
        DEEP_DIR_URL
      }
    val ds = first
      .explore(S"root directory URI".text)(
        Wget('A)
      )
      .persist()

    assert(ds.bottleneckRDD.count() == 3)
    assert(ds.spooky.spookyMetrics.pagesFetched.value == 4)

    assert(ds.rdd.count() == 3)
    assert(ds.spooky.spookyMetrics.pagesFetched.value <= 4)
  }

  describe("When using custom keyBy function, explore plan can") {

    it("avoid fetching traces with identical TraceView and preserve keyBy in its output") {
      val first = spooky
        .wget {
          DEEP_DIR_URL
        }
      val ds = first
        .explore(S"root directory URI".text)(
          Wget('A),
          keyBy = ExplorePlanSpec.CustomKeyBy
        )
        .persist()

      assert(ds.bottleneckRDD.count() == 1)
      assert(ds.spooky.spookyMetrics.pagesFetched.value == 2)

      assert(ds.rdd.count() == 1)
      assert(ds.spooky.spookyMetrics.pagesFetched.value <= 2)

      val rows = ds.bottleneckRDD.collect()

      rows.foreach { row =>
        assert(row.traceView.samenessDelegatedTo === ExplorePlanSpec.CustomKeyBy(row.traceView))
      }
    }
  }
}

object ExplorePlanSpec {

  object CustomKeyBy extends (List[Action] => Any) with Serializable {

    override def apply(actions: List[Action]): Any = {

      val uris = actions.collect {
        case v: Wget => v.uri.value
      }
      val parts = uris.head.split('/')
      val key = parts.slice(parts.length - 2, parts.length).toList
      key
    }
  }
}
