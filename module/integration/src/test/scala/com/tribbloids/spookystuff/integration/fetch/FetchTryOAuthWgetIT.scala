package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.UncacheableIntegrationFixture
import org.apache.spark.SparkException

/**
  * Created by peng on 11/26/14.
  */
class FetchTryOAuthWgetIT extends UncacheableIntegrationFixture {

  override lazy val webDriverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val spooky = this.spooky
    import spooky.dsl._

    val RDD = sc
      .parallelize(Seq("http://malformed uri"))
      .fetch(
        ClusterRetry(OAuthV2(Wget('_)), 3)
      )
      .extract(S.code ~ 'page)
      .persist()

    assert(RDD.fetchedRDD.first().onlyDoc.isEmpty)

    val pageRows = RDD.toStringRDD('page).collect()
    assert(pageRows sameElements Array(null))

    // max cluster-wise retries defaults to 4 times, smaller than 5 but bigger than 3
    intercept[SparkException] {
      sc.parallelize(Seq("http://malformed uri"))
        .fetch(
          ClusterRetry(OAuthV2(Wget('_)), 5)
        )
        .extract(S.code ~ 'page)
        .collect()
    }
  }

  override def numPages: Long = 0

  override def numSessions: Long = 1 // TODO: should be 6, why local retry and cluster-wise retry doesn't count?
}
