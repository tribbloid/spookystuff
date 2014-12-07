//package org.tribbloid.spookystuff.integration
//
//import org.tribbloid.spookystuff.actions.Visit
//import org.tribbloid.spookystuff.dsl._
//import org.tribbloid.spookystuff.{SpookyContext, dsl}
//
///**
//  * Created by peng on 12/5/14.
//  */
//class ExploreIT extends IntegrationSuite {
//   override def doMain(spooky: SpookyContext): Unit = {
//
//     import spooky._
//
//     val base = noInput
//       .fetch(
//         Visit("http://webscraper.io/test-sites/e-commerce/allinone")
//       )
//
//     val explored = base
//       .explore($"div.sidebar-nav a", indexKey = 'i1)(
//         Visit('A.href),
//         depthKey = 'depth
//       )(
//         'A.text > 'category
//       )
//       .select($"h1".text > 'header)
//       .asSchemaRDD()
//
//     assert(
//       explored.schema.fieldNames ===
//         "i1" ::
//           "category" ::
//           "depth" ::
//           "header" :: Nil
//     )
//
//     val rows = explored.collect()
//     assert(rows.size === 6)
//
//     assert(rows(0).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops")
//     assert(rows(1).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets")
//     assert(rows(2).mkString("|") === "2|Phones|0|Touch|Phones / Touch")
//   }
//
//   override def numPages: Int = 7
//}
