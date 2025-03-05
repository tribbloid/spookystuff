//package com.tribbloids.spookystuff.execution
//
//import ai.acyclic.prover.commons.debug.print_@
//import com.tribbloids.spookystuff.actions.{Trace, Wget}
//import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
//import com.tribbloids.spookystuff.{dsl, QueryException}
//import org.apache.spark.HashPartitioner
//
///**
//  * Created by peng on 05/04/16.
//  */
//class ExplorePlanSpec extends SpookyBaseSpec with FileDocsFixture {
//
//  import dsl._
//
//  ignore("toString") { // de-prioritised
//
//    val base = spooky
//      .fetch(_ => Wget("dummy"))
//
//    val explored = base
//      .explore(S"div.sidebar-nav a", ordinalField = 'index)(
//        Wget('A.href),
//        depthField = 'depth
//      )
//      .select(row =>
//        row
////        'A.text ~ 'category,
////        S"h1".text ~ 'header
//      )
//
//    print_@(explored.plan.toString)
//  }
//
//  it("should create a new beaconRDD if its upstream doesn't have one") {
//    val partitioner = new HashPartitioner(8)
//
//    val src = spooky
//      .extract(Lit("abc") ~ 'dummy)
//
//    assert(src.plan.beaconRDDOpt.isEmpty)
//
//    val rdd1 = src
//      .explore('dummy)(
//        Wget(HTML_URL),
//        genPartitioner = GenPartitioners.DocCacheAware(Impl { _ =>
//          partitioner
//        })
//      )
//
//    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
//  }
//
//  it("should inherit old beaconRDD from upstream if exists") {
//    val partitioner = new HashPartitioner(8)
//    val partitioner2 = new HashPartitioner(16)
//
//    val rdd1 = spooky
//      .extract(Lit("abc") ~ 'dummy)
//      .explore('dummy)(
//        Wget(HTML_URL),
//        genPartitioner = GenPartitioners.DocCacheAware(Impl { _ =>
//          partitioner
//        })
//      )
//
//    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
//    val beaconRDD = rdd1.plan.beaconRDDOpt.get
//
//    val rdd2 = rdd1
//      .explore('dummy)(
//        Wget(HTML_URL),
//        genPartitioner = GenPartitioners.DocCacheAware(Impl { _ =>
//          partitioner2
//        })
//      )
//
//    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
//    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
//  }
//
//  describe("should work on directory, range:") {
//
//    val resourcePath = DEEP_DIR_URL
//
//    def computeFrom(range: Range) = {
//
//      val df = spooky
//        .create(Seq(resourcePath))
//        .fetch {
//          Wget('_)
//        }
//        .explore(
//          S"root directory URI".text
//        )(
//          Wget('A),
//          depthField = 'depth,
//          range = range
//        )
//        .fork(S"root file", ordinalField = 'index)(
//          A"name".text into 'leaf,
//          A"URI".text ~ 'fullPath
//        )
//        .toDF(sort = true)
//
//      val result = df.collect().toList
//      result
//    }
//
//    lazy val bigInt = Int.MaxValue - 10
//
//    lazy val `0..` = computeFrom(0 to bigInt)
//
//    lazy val `-1..` = computeFrom(-1 to bigInt)
//
//    it("from -1") {
//
//      `-1..`.mkString("\n")
//        .shouldBe(
//          """
//            |[/tmp/spookystuff/resources/testutils/dir,null,null,null,null]
//            |[/tmp/spookystuff/resources/testutils/dir,0,ArraySeq(0),ArraySeq(hivetable.csv),file:///tmp/spookystuff/resources/testutils/dir/hivetable.csv]
//            |[/tmp/spookystuff/resources/testutils/dir,0,ArraySeq(1),ArraySeq(table.csv),file:///tmp/spookystuff/resources/testutils/dir/table.csv]
//            |[/tmp/spookystuff/resources/testutils/dir,1,ArraySeq(0, 0),ArraySeq(hivetable.csv, Test.pdf),file:///tmp/spookystuff/resources/testutils/dir/dir/Test.pdf]
//            |[/tmp/spookystuff/resources/testutils/dir,2,ArraySeq(0, 0, 0),ArraySeq(hivetable.csv, Test.pdf, pom.xml),file:///tmp/spookystuff/resources/testutils/dir/dir/dir/pom.xml]
//            |[/tmp/spookystuff/resources/testutils/dir,3,ArraySeq(0, 0, 0, 0),ArraySeq(hivetable.csv, Test.pdf, pom.xml, tribbloid.json),file:///tmp/spookystuff/resources/testutils/dir/dir/dir/dir/tribbloid.json]
//            |""".stripMargin
//        )
//    }
//
//    it("from 0") {
//
//      `0..`.mkString("\n")
//        .shouldBe(
//          `-1..`.slice(1, bigInt).mkString("\n")
//        )
//    }
//
//    it("0 to 2") {
//
//      computeFrom(0 to 2)
//        .mkString("\n")
//        .shouldBe(
//          `0..`.slice(0, 3).mkString("\n")
//        )
//    }
//
//    it("from 2") {
//
//      computeFrom(2 to bigInt)
//        .mkString("\n")
//        .shouldBe(
//          `0..`.slice(3, bigInt).mkString("\n")
//        )
//    }
//
//    it("2 to 2") {
//
//      computeFrom(2 to 3)
//        .mkString("\n")
//        .shouldBe(
//          `0..`.slice(3, 4).mkString("\n")
//        )
//    }
//
//  }
//
//  it("should throw an exception if OrdinalField == DepthField") {
//    val rdd1 = spooky
//      .fetch {
//        Wget(HTML_URL)
//      }
//
//    intercept[QueryException] {
//      rdd1
//        .explore(S"root directory".attr("path"), ordinalField = 'dummy)(
//          Wget('A),
//          depthField = 'dummy
//        )
//    }
//  }
//
//  it("should avoid shuffling the latest batch to minimize repeated fetch") {
//    val first = spooky
//      .wget {
//        DEEP_DIR_URL
//      }
//
//    val ds = first
//      .explore(S"root directory URI".text)(
//        Wget('A)
//      )
//      .persist()
//
//    object clue {
//      override def toString: String = {
//        ds.toDF(sort = true, removeTransient = false).show(false)
//        ds.treeString
//      }
//    }
//
//    assert(ds.squashedRDD.count() == 4, clue)
//
//    assert(ds.spooky.spookyMetrics.pagesFetched.value == 4)
//
//    assert(ds.rdd.count() == 4, clue)
//    assert(ds.spooky.spookyMetrics.pagesFetched.value <= 4)
//  }
//
//  describe("When using custom keyBy function, explore plan can") {
//
//    it("avoid fetching traces with identical Trace and preserve keyBy in its output") {
//      val first = spooky
//        .wget {
//          DEEP_DIR_URL
//        }
//      val ds = first
//        .explore(S"root directory URI".text)(
//          Wget('A),
//          keyBy = ExplorePlanSpec.CustomKeyBy
//        )
//        .persist()
//
//      object clue {
//        override def toString: String = {
//          ds.toDF(sort = true, removeTransient = false).show(false)
//          ds.treeString
//        }
//      }
//
//      assert(ds.squashedRDD.count() == 2, clue)
//      assert(ds.spooky.spookyMetrics.pagesFetched.value == 2, clue)
//
//      assert(ds.rdd.count() == 2, clue)
//      assert(ds.spooky.spookyMetrics.pagesFetched.value <= 2, clue)
//
//      val rows = ds.squashedRDD.collect()
//
//      rows.foreach { row =>
//        assert(row.group.samenessKey === ExplorePlanSpec.CustomKeyBy(row.group))
//      }
//    }
//  }
//}
//
//object ExplorePlanSpec {
//
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
//}
