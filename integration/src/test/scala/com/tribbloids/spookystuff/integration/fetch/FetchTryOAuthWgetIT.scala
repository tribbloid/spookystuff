package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.UncacheableIntegrationFixture
import org.apache.spark.SparkException

/**
 * Created by peng on 11/26/14.
 */
class FetchTryOAuthWgetIT extends UncacheableIntegrationFixture {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain() {

    val spooky = this.spooky
    import com.tribbloids.spookystuff.utils.SpookyViews._
    import spooky.dsl._

    val RDD = sc.parallelize(Seq("http://malformed uri"))
      .fetch(
        Try(OAuthV2(Wget('_)),3)
      )
      .select(S.code ~ 'page)
      .persist()

    assert(RDD.unsquashedRDD.first().getOnlyPage.isEmpty)

    val pageRows = RDD.toStringRDD('page).collect()
    assert(pageRows sameElements Array(null))

    // max cluster-wise retries defaults to 4 times, smaller than 5 but bigger than 3
    intercept[SparkException]{
      sc.parallelize(Seq("http://malformed uri"))
        .fetch(
          Try(OAuthV2(Wget('_)),5)
        )
        .select(S.code ~ 'page)
        .collect()
    }
  }

  override def numPages= 0

  override def numSessions = 3 //TODO: should be 6, why local retry and cluster-wise retry doesn't count?

  override def numDrivers = 0
}