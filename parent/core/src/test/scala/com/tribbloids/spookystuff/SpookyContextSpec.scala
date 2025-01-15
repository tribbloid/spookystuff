package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.spark.serialization.AssertSerializable
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.conf.{Core, Dir, SpookyConf}
import com.tribbloids.spookystuff.rdd.SpookyDataset
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, SpookyBaseSpec}

class SpookyContextSpec extends SpookyBaseSpec {

  lazy val resources: RemoteDocsFixture.type = RemoteDocsFixture
  import resources.*

  it("SpookyContext should be Serializable") {

    val spooky: SpookyContext = this.spooky
    spooky.Plugins.registerEnabled()

    AssertSerializable[SpookyContext](spooky)
      .on(
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

    AssertSerializable(
      spooky.dsl
    )
      .weakly()
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
        .fetch(_ => Wget(HTML_URL))
      d1.fetchedRDD.count()

      val d2 = spooky
        .fetch(_ => Wget(HTML_URL))
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
        .fetch(_ => Wget(HTML_URL))
      rdd1.count()

      val rdd2 = spooky
        .fetch(_ => Wget(HTML_URL))
      rdd2.count()

      rdd1.spooky.spookyMetrics.toTreeIR.treeView.treeString shouldBe
        rdd2.spooky.spookyMetrics.toTreeIR.treeView.treeString
    }
  }

  describe("create from") {

    it("Seq") {

      val rdd: SpookyDataset[String] = spooky.create(Seq("a", "b"))

      val data = rdd.squashedRDD.collect().flatMap(_.batch).toList
      assert(data == List("a", "b"))
    }
  }

  it("default SpookyContext should have default dir configs") {

    val context = SpookyContext(this.sql)

    val dirs = context.dirConf
    val json = dirs.prettyJSON
    println(json)

    import dirs.*
    assert(
      !Seq(
        root,
        localRoot,
        auditing,
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

    import dirs.*
    assert(
      !Seq(
        root,
        localRoot,
        auditing,
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

    import dirs.*
    assert(
      !Seq(
        root,
        localRoot,
        auditing,
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
