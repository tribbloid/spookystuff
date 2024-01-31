package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.conf.{Core, Dir, SpookyConf}
import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.utils.serialization.AssertWeaklySerializable

class SpookyContextSpec extends SpookyBaseSpec with LocalPathDocsFixture {

  it("SpookyContext should be Serializable") {

    val spooky: SpookyContext = this.spooky
    spooky.Plugins.registerEnabled()

    AssertWeaklySerializable[SpookyContext](
      spooky,
      condition = { (v1, v2) =>
        Seq(v1, v2)
          .map { _ =>
            v1.Plugins.cached.lookup.values
          }
          .reduce { (v1, v2) =>
            assert(v1 == v2)
            v1
          }

      }
    )
  }

  it("SpookyContext.dsl should be Serializable") {

    val spooky = this.spooky

    AssertWeaklySerializable(
      spooky.dsl
    )
  }

  it("derived instances of a SpookyContext should have the same configuration") {

    val spooky = this.spooky
    spooky(Core).confUpdate(_.copy(shareMetrics = false))

    val rdd2 = spooky.create(Seq("dummy"))
    assert(!(rdd2.spooky eq spooky))

    val conf1 = spooky.dirConf.prettyJSON
    val conf2 = rdd2.spooky.dirConf.prettyJSON
    conf1 shouldBe conf2
  }

  it("derived instances of a SpookyContext should have the same configuration after it has been modified") {

    val spooky = this.spooky
    spooky(Core).confUpdate(_.copy(shareMetrics = false))

    spooky(Dir).confUpdate(
      _.copy(
        root = "s3://root",
        cache = "hdfs://dummy"
      )
    )

    val rdd2 = spooky.create(Seq("dummy"))
    assert(!(rdd2.spooky eq spooky))

    val conf1 = spooky.dirConf.prettyJSON
    val conf2 = rdd2.spooky.dirConf.prettyJSON
    conf1 shouldBe conf2
  }

  describe("each execution should have") {

    it("independent metrics if sharedMetrics=false") {

      val spooky = this.spooky
      spooky(Core).confUpdate(_.copy(shareMetrics = false))

      val d1 = spooky
        .fetch(
          Wget(HTML_URL)
        )
      d1.fetchedRDD.count()

      val d2 = spooky
        .fetch(
          Wget(HTML_URL)
        )
      d2.fetchedRDD.count()

      val seq = Seq(d1, d2)

      val metrics = seq.map(_.spooky.spookyMetrics)

      metrics
        .map(_.View.toMap)
        .reduce { (v1, v2) =>
          v1 mapShouldBe v2
          assert(!v1.eq(v2))
          v1
        }

      seq.foreach { d =>
        assert(d.spooky.spookyMetrics.pagesFetched.value === 1)
      }
    }

    it("shared metrics if sharedMetrics=true") {

      val spooky = this.spooky
      spooky(Core).confUpdate(_.copy(shareMetrics = true))

      val rdd1 = spooky
        .fetch(
          Wget(HTML_URL)
        )
      rdd1.count()

      val rdd2 = spooky
        .fetch(
          Wget(HTML_URL)
        )
      rdd2.count()

      rdd1.spooky.spookyMetrics.toTreeIR.treeView.treeString shouldBe
        rdd2.spooky.spookyMetrics.toTreeIR.treeView.treeString
    }
  }

  it("can create PageRow from String") {

    val spooky = this.spooky
    val rdd = spooky.create(Seq("a", "b"))

    val data = rdd.squashedRDD.collect().flatMap(_.dataRows).map(_.data).toList
    assert(data == List(Map(Field("_") -> "a"), Map(Field("_") -> "b")))
  }

  it("can create PageRow from map[String, String]") {

    val spooky = this.spooky
    val rdd = spooky.create(Seq(Map("1" -> "a"), Map("2" -> "b")))

    val data = rdd.squashedRDD.collect().flatMap(_.dataRows).map(_.data).toList
    assert(data == List(Map(Field("1") -> "a"), Map(Field("2") -> "b")))
  }

  it("can create PageRow from map[Symbol, String]") {

    val spooky = this.spooky
    val rdd = spooky.create(Seq(Map('a1 -> "a"), Map('a2 -> "b")))

    val data = rdd.squashedRDD.collect().flatMap(_.dataRows).map(_.data).toList
    assert(data == List(Map(Field("a1") -> "a"), Map(Field("a2") -> "b")))
  }

  it("can create PageRow from map[Int, String]") {

    val spooky = this.spooky
    val rdd = spooky.create(Seq(Map(1 -> "a"), Map(2 -> "b")))

    val data = rdd.squashedRDD.collect().flatMap(_.dataRows).map(_.data).toList
    assert(data == List(Map(Field("1") -> "a"), Map(Field("2") -> "b")))
  }

  it("default SpookyContext should have default dir configs") {

    val context = new SpookyContext(this.sql)

    val dirs = context.dirConf
    val json = dirs.prettyJSON
    println(json)

    import dirs._
    assert(
      !Seq(
        root,
        localRoot,
        autoSave,
        cache,
        errorDump,
        errorScreenshot,
        checkpoint,
        errorDumpLocal,
        errorScreenshotLocal
      ).contains(null)
    )
  }

  it("when sharedMetrics=false, new SpookyContext created from default SpookyConf should have default dir configs") {

    val conf: SpookyConf = new SpookyConf(shareMetrics = false)
    val context = SpookyContext(this.sql, conf)

    val dirs = context.dirConf
    val json = dirs.prettyJSON
    println(json)

    import dirs._
    assert(
      !Seq(
        root,
        localRoot,
        autoSave,
        cache,
        errorDump,
        errorScreenshot,
        checkpoint,
        errorDumpLocal,
        errorScreenshotLocal
      ).contains(null)
    )
  }

  it("when sharedMetrics=true, new SpookyContext created from default SpookyConf should have default dir configs") {

    val conf: SpookyConf = new SpookyConf(shareMetrics = true)
    val context = SpookyContext(this.sql, conf)

    val dirs = context.dirConf
    val json = dirs.prettyJSON
    println(json)

    import dirs._
    assert(
      !Seq(
        root,
        localRoot,
        autoSave,
        cache,
        errorDump,
        errorScreenshot,
        checkpoint,
        errorDumpLocal,
        errorScreenshotLocal
      ).contains(null)
    )
  }
}
