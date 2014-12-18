//package org.tribbloid.spookystuff.integration
//
//import org.tribbloid.spookystuff.actions._
//import org.tribbloid.spookystuff.dsl._
//import org.tribbloid.spookystuff.{SpookyContext, dsl}
//
//import scala.concurrent.duration
//import scala.concurrent.duration._
//
///**
//* Created by peng on 12/10/14.
//*/
//class ExploreLoadMoreIT extends IntegrationSuite {
//  override def doMain(spooky: SpookyContext): Unit = {
//    import spooky._
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
//      .explore($"div.sidebar-nav a", depthKey = 'depth, indexKey = 'i1)(
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
//      .asSchemaRDD()
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