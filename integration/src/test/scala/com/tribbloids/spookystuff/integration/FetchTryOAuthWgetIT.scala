package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import org.apache.spark.SparkException

/**
 * Created by peng on 11/26/14.
 */
class FetchTryOAuthWgetIT extends UncacheableIntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    import spooky.dsl._

    val RDD = sc.parallelize(Seq("http://malformed uri"))
      .fetch(
        Try(OAuthV2(Wget('_)),3)
      )
      .select(S.code ~ 'page)
      .persist()
    //
    assert(RDD.first().getOnlyPage.isEmpty)

    val pageRows = RDD.toStringRDD('page).collect()
    assert(pageRows.length == 0)

    intercept[SparkException]{
      val RDD = sc.parallelize(Seq("http://malformed uri"))
        .fetch(
          Try(OAuthV2(Wget('_)),5)
        )
        .select(S.code ~ 'page)
        .collect()
    }
  }

  override def numFetchedPages = {_ => 0}

  override def numSessions = 1 //TODO: should be 6, why local retry and cluster-wise retry doesn't count?

  override def numDrivers = 0
}