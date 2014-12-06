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
//     val joined = base
//       .join($"div.sidebar-nav a")(
//         Visit('A.href)
//       )(
//         'A.text > 'category
//       )
//       .join($"a.subcategory-link")(
//         Visit('A.href)
//       )(
//         'A.text > 'subcategory
//       )
//       .select($"h1" > 'header)
//       .asSchemaRDD()
//
//     val rows = joined.collect()
//     assert(rows.size === 3)
//
// //    assert()
//   }
//
//   override def expectedPages: Int = ???
// }
