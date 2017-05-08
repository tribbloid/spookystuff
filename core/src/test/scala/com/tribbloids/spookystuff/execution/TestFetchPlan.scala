package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.extractors.Lit
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.{SpookyEnvFixture, dsl}
import org.apache.spark.HashPartitioner

/**
  * Created by peng on 02/04/16.
  */
class TestFetchPlan extends SpookyEnvFixture with LocalPathDocsFixture {

  import dsl._

  it("FetchPlan.toString should work") {

    val rdd1 = spooky
      .fetch(
        Wget(HTML_URL)
      )

    println(rdd1.plan.toString)
  }

  it("FetchPlan is lazy and doesn't immediately do the fetch"){

    val rdd1 = spooky
      .fetch(
        Wget(HTML_URL)
      )
    rdd1.dataRDD.count()

    assert(rdd1.spooky.metrics.pagesFetched.value === 0)
  }

  it("fetch() + count() will fetch once") {

    val rdd1 = spooky
      .fetch(
        Wget(HTML_URL)
      )

    rdd1.rdd.count()

    assert(rdd1.spooky.metrics.pagesFetched.value === 1)
  }

  it("fetch() + select() + count() will fetch once") {

    val rdd1 = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .select(
        Lit("Wikipedia") ~ 'name
      )

    rdd1.unsquashedRDD.count()

    assert(rdd1.spooky.metrics.pagesFetched.value === 1)
  }

  it("FetchPlan should create a new beaconRDD if its upstream doesn't have one"){
    val partitioner = new HashPartitioner(8)

    val src = spooky
      .extract(Lit("abc") ~ 'dummy)

    assert(src.plan.beaconRDDOpt.isEmpty)

    val rdd1 = src
      .fetch(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner)
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
  }


  it("FetchPlan should inherit old beaconRDD from upstream if exists") {
    val partitioner = new HashPartitioner(8)
    val partitioner2 = new HashPartitioner(16)

    val rdd1 = spooky
      .extract(Lit("abc") ~ 'dummy)
      .fetch(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner)
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    val beaconRDD = rdd1.plan.beaconRDDOpt.get

    val rdd2 = rdd1
      .fetch(
        Wget(HTML_URL),
        genPartitioner = GenPartitioners.DocCacheAware(_ => partitioner2)
      )

    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
  }
}
