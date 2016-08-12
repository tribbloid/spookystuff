package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.UncacheableIntegrationSuite
import org.apache.spark.SparkException

/**
 * Created by peng on 11/26/14.
 */
class FetchTryOAuthWgetIT extends UncacheableIntegrationSuite {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain() {

    val spooky = this.spooky
    import com.tribbloids.spookystuff.utils.ImplicitUtils._
    import spooky.dsl._

    val RDD = sc.parallelize(Seq("http://malformed uri"))
      .fetch(
        Try(OAuthV2(Wget('_)),3)
      )
      .select(S.code ~ 'page)
      .persist()
    //
    assert(RDD.unsquashedRDD.first().getOnlyPage.isEmpty)

    val pageRows = RDD.toStringRDD('page).collect()
    assert(pageRows sameElements Array(null))

    intercept[SparkException]{
      val RDD = sc.parallelize(Seq("http://malformed uri"))
        .fetch(
          Try(OAuthV2(Wget('_)),5)
        )
        .select(S.code ~ 'page)
        .collect()
    }
  }

  override def numPages= 0

  override def numSessions = 1 //TODO: should be 6, why local retry and cluster-wise retry doesn't count?

  override def numDrivers = 0
}