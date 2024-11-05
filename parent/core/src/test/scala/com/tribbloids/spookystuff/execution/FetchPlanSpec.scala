//package com.tribbloids.spookystuff.execution
//
//import ai.acyclic.prover.commons.function.Impl
//import com.tribbloids.spookystuff.actions.Wget
//import com.tribbloids.spookystuff.commons.serialization.AssertWeaklySerializable
//import com.tribbloids.spookystuff.extractors.impl.Lit
//import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
//import com.tribbloids.spookystuff.dsl
//import org.apache.spark.HashPartitioner
//
///**
//  * Created by peng on 02/04/16.
//  */
//class FetchPlanSpec extends SpookyBaseSpec with FileDocsFixture {
//
//  import dsl._
//
//  it("FetchPlan should be serializable") {
//
//    val rdd1 = spooky
//      .fetch(
//        Wget(HTML_URL)
//      )
//
//    AssertWeaklySerializable(rdd1.plan)
//  }
//
//  it("FetchPlan.toString should work") {
//
//    val rdd1 = spooky
//      .fetch(
//        Wget(HTML_URL)
//      )
//
//    println(rdd1.plan.toString)
//  }
//
//  it("fetch() + count() will fetch once") {
//
//    val rdd1 = spooky
//      .fetch(
//        Wget(HTML_URL)
//      )
//
//    rdd1.rdd.count()
//
//    assert(rdd1.spooky.spookyMetrics.pagesFetched.value === 1)
//  }
//
//  it("fetch() + select() + count() will fetch once") {
//
//    val rdd1 = spooky
//      .fetch(
//        Wget(HTML_URL)
//      )
//      .extract(
//        Lit("Wikipedia") ~ 'name
//      )
//
//    rdd1.fetchedRDD.count()
//
//    assert(rdd1.spooky.spookyMetrics.pagesFetched.value === 1)
//  }
//
//  it("FetchPlan should create a new beaconRDD if its upstream doesn't have one") {
//    val partitioner = new HashPartitioner(8)
//
//    val src = spooky
//      .extract(Lit("abc") ~ 'dummy)
//
//    assert(src.plan.beaconRDDOpt.isEmpty)
//
//    val rdd1 = src
//      .fetch(
//        Wget(HTML_URL),
//        genPartitioner = GenPartitioners.DocCacheAware(Impl { _ =>
//          partitioner
//        })
//      )
//
//    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
//  }
//
//  it("FetchPlan should inherit old beaconRDD from upstream if exists") {
//    val partitioner = new HashPartitioner(8)
//    val partitioner2 = new HashPartitioner(16)
//
//    val rdd1 = spooky
//      .extract(Lit("abc") ~ 'dummy)
//      .fetch(
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
//      .fetch(
//        Wget(HTML_URL),
//        genPartitioner = GenPartitioners.DocCacheAware(Impl { _ =>
//          partitioner2
//        })
//      )
//
//    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
//    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
//  }
//}
