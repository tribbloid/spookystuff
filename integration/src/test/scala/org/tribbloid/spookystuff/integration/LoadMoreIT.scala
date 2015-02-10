//package org.tribbloid.spookystuff.integration
//
//import org.tribbloid.spookystuff.SpookyContext
//import org.tribbloid.spookystuff.actions._
//import org.tribbloid.spookystuff.dsl._
//
//import scala.concurrent.duration._
//
///**
// * Created by peng on 12/10/14.
// */
//class LoadMoreIT extends IntegrationSuite {
//  override def doMain(spooky: SpookyContext): Unit = {
//    spooky.autoSave = false
//    spooky.driverFactory = NaiveDriverFactory(loadImages = true)
//
//    import spooky._
//
//    val result = noInput
//      .fetch(
//        Visit("http://webscraper.io/test-sites/e-commerce/more/computers/tablets")
//          +> Loop(
//          Click("a.btn") :: Delay(2.seconds) :: Screenshot().as('scr):: Nil
//          ,10
//        )
//          +> Snapshot().as('~),
//        flattenPagesOrdinalKey = 'times
//      )
//      .select(
//        '~.children("a.title").size > 'num_product
//      )
//      .save(
//        x"file://${System.getProperty("user.home")}/spooky-integration/images/${'times}"
//      )
//      .asSchemaRDD()
//
//    assert(
//      result.schema.fieldNames ===
//        "num_product" :: Nil
//    )
//    val rows = result.collect()
//    assert(rows.size === 1)
//    assert(rows(0).getInt(0) === 21)
//  }
//
//  override def numPages: Int = 1
//}