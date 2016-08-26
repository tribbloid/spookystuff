package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.tests.LocalPathDocsFixture
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{QueryException, SpookyEnvFixture, dsl}
import org.apache.spark.HashPartitioner

/**
  * Created by peng on 05/04/16.
  */
class TestExplorePlan extends SpookyEnvFixture with LocalPathDocsFixture {

  import dsl._

  test("ExplorePlan.toString should work") {

    val base = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val explored = base
      .explore(S"div.sidebar-nav a", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'depth
      )(
        'A.text ~ 'category,
        S"h1".text ~ 'header
      )

    println(explored.plan.toString)
  }

  test("ExplorePlan should create a new beaconRDD if its upstream doesn't have one"){
    val partitioner = new HashPartitioner(8)

    val src = spooky
      .extract("abc" ~ 'dummy)

    assert(src.plan.beaconRDDOpt.isEmpty)

    val rdd1 = src
      .explore('dummy)(
        Wget(HTML_URL),
        fetchOptimizer = FetchOptimizers.WebCacheAware,
        partitionerFactory = {v => partitioner}
      )()

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
  }


  test("ExplorePlan should inherit old beaconRDD from upstream if exists") {
    val partitioner = new HashPartitioner(8)
    val partitioner2 = new HashPartitioner(16)

    val rdd1 = spooky
      .extract("abc" ~ 'dummy)
      .explore('dummy)(
        Wget(HTML_URL),
        fetchOptimizer = FetchOptimizers.WebCacheAware,
        partitionerFactory = {v => partitioner}
      )()

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    val beaconRDD = rdd1.plan.beaconRDDOpt.get

    val rdd2 = rdd1
      .explore('dummy)(
        Wget(HTML_URL),
        fetchOptimizer = FetchOptimizers.WebCacheAware,
        partitionerFactory = {v => partitioner2}
      )()

    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
  }

  test("ExplorePlan should work recursively on directory") {

    val resourcePath = DIR_URL

    val df = spooky.create(Seq(resourcePath.toString))
      .fetch{
        Wget('_)
      }
      .explore(S"root directory".attr("path"))(
        Wget('A)
      )()
      .flatExtract(S"root file")(
        'A.ownText ~ 'leaf,
        'A.attr("path") ~ 'fullPath,
        'A.allAttr ~ 'metadata
      )
      .toDF(sort = true)

    df.show(truncate = false)
  }

  test("ExplorePlan will throw an exception if OrdinalField == DepthField") {
    val rdd1 = spooky
      .fetch{
        Wget(HTML_URL)
      }

    intercept[QueryException] {
      rdd1
        .explore(S"root directory".attr("path"), ordinalField = 'dummy)(
          Wget('A),
          depthField = 'dummy
        )()
    }
  }
}