//package com.tribbloids.spookystuff.integration
//
//import com.tribbloids.spookystuff.actions._
//import com.tribbloids.spookystuff.dsl._
//import com.tribbloids.spookystuff.{SpookyContext, dsl}
//
//import scala.concurrent.duration
//import scala.concurrent.duration._
//
///**
//* Created by peng on 12/10/14.
//*/
//class ExploreLoadMoreIT extends IntegrationSuite {
//  override def doMain(spooky: SpookyContext): Unit = {
//    import spooky.dsl._
//
//    val base = noInput
//      .fetch(
//        Visit("http://webscraper.io/test-sites/e-commerce/more")
//          +> Loop(
//          Click("a.btn") :: Delay(2.seconds) :: Nil
//        )
//          +> Snapshot()
//      )
//
//    val result = base
//      .explore($"div.sidebar-nav a", depthField = 'depth, ordinalField = 'i1)(
//        Visit('A.href)
//          +> Loop(
//          Click("a.btn") :: Delay(2.seconds) :: Nil
//        )
//          +> Snapshot()
//      )(
//        'A.text > 'category,
//        'first.children("h1").text > 'title,
//        $"a.title".size > 'num_product
//      )
//      .asDataFrame()
//
//    assert(
//      result.schema.fieldNames ===
//        "depth" ::
//          "i1" ::
//          "category" ::
//          "title" ::
//          "num_product" :: Nil
//    )
//
//    val rows = result.collect()
//    assert(rows.size === 6)
//  }
//
//  override def numSessions = 6
//
//  override def numPages: Int = 6
//
//  override def numDrivers = 6
//}