package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.dsl
import org.apache.spark.HashPartitioner
import ai.acyclic.prover.commons.spark.serialization.AssertSerializable

/**
  * Created by peng on 02/04/16.
  */
class FetchPlanSpec extends SpookyBaseSpec {

  import dsl._
  import FileDocsFixture.*

  it("FetchPlan should be serializable") {

    val rdd1 = spooky
      .fetch(_ => Wget(HTML_URL))

    AssertSerializable(rdd1.plan).weakly()
  }

  it("FetchPlan.toString should work") {

    val rdd1 = spooky
      .fetch(_ => Wget(HTML_URL))

    rdd1.plan.toString.shouldBe()
  }

  it("fetch() + count() will fetch once") {

    val rdd1 = spooky
      .fetch(_ => Wget(HTML_URL))

    rdd1.rdd.count()

    assert(rdd1.ctx.spookyMetrics.pagesFetched.value === 1)
  }

  it("fetch() + select() + count() will fetch once") {

    val rdd1 = spooky
      .fetch(_ => Wget(HTML_URL))
      .select(_ => "Wikipedia")

    rdd1.rdd.count()

    assert(rdd1.ctx.spookyMetrics.pagesFetched.value === 1)
  }

  it("FetchPlan should create a new beaconRDD if its upstream doesn't have one") {
    val partitioner = new HashPartitioner(8)

    val src = spooky
      .select(_ => "abc")

    assert(src.plan.beaconRDDOpt.isEmpty)

    val rdd1 = src
      .fetch(
        _ => Wget(HTML_URL),
        genPartitioner = Locality.DocCacheAware { _ =>
          partitioner
        }
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
  }

  it("FetchPlan should inherit old beaconRDD from upstream if exists") {
    val partitioner = new HashPartitioner(8)
    val partitioner2 = new HashPartitioner(16)

    val rdd1 = spooky
      .select(_ => "abc")
      .fetch(
        _ => Wget(HTML_URL),
        genPartitioner = Locality.DocCacheAware { _ =>
          partitioner
        }
      )

    assert(rdd1.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    val beaconRDD = rdd1.plan.beaconRDDOpt.get

    val rdd2 = rdd1
      .fetch(
        _ => Wget(HTML_URL),
        genPartitioner = Locality.DocCacheAware { _ =>
          partitioner2
        }
      )

    assert(rdd2.plan.beaconRDDOpt.get.partitioner.get eq partitioner)
    assert(rdd2.plan.beaconRDDOpt.get eq beaconRDD)
  }
}
