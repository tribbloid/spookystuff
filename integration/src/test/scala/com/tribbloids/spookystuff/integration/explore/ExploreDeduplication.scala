//package com.tribbloids.spookystuff.integration
//
//import com.tribbloids.spookystuff.SpookyContext
//import com.tribbloids.spookystuff.dsl._
//
///**
// * Created by peng on 12/08/15.
// */
//class ExploreDeduplication extends IntegrationSuite {
//
//  override lazy val drivers = Seq(
//    null
//  )
//
//  override def doMain(spooky: SpookyContext): Unit = {
//    val uris = spooky.wget("http://dbpedia.org/page/Rob_Ford")
//      .wgetExplore(
//        S"""a[rel^=dbo][href*=dbpedia],a[rev^=dbo][href*=dbpedia]""".hrefs.distinct.slice(0,9),
//        failSafe = 2,
//        depthField = 'depth,
//        maxDepth = 2
////        select = S.uri ~ 'name
//      )
//    .toStringRDD(S.uid.andMap(_.backtrace)).collect()
//
//    assert{
//      uris.distinct.length == uris.length
//    }
//
//  }
//
//  override def numPages: (QueryOptimizer) => Int = _ => 10
//}
