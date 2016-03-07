//package com.tribbloids.spookystuff.integration
//
//import com.tribbloids.spookystuff.SpookyContext
//import com.tribbloids.spookystuff.actions._
//import com.tribbloids.spookystuff.dsl._
//
///**
// * Created by peng on 12/10/14.
//TODO: this will be enabled after hidden key integration
// */
//class NullOrdinalAnddepthFieldsIT extends IntegrationSuite {
//
//  override lazy val drivers = Seq(
//    null
//  )
//
//  override def doMain(spooky: SpookyContext): Unit = {
//
//    val joined = spooky
//      .fetch(
//        Wget("http://webscraper.io/test-sites/e-commerce/static")
//      )
//      .join($"div.sidebar-nav a", ordinalField = null)(
//        Wget('A.href),
//        joinType = LeftOuter
//      )(
//        'A.text ~ 'category
//      )
//      .join($"a.subcategory-link", ordinalField = null)(
//        Wget('A.href),
//        joinType = LeftOuter
//      )(
//        'A.text ~ 'subcategory
//      )
//      .select($"h1".text ~ 'header)
//
//    val result = joined
//      .explore($"ul.pagination a", depthField = null, ordinalField = null)(
//        Wget('A.href)
//      )(
//        'A.text as 'page
//      )
//      .select($.uri ~ 'uri)
//      .toDF(sort = true)
//      .persist()
//
//    assert(
//      result.schema.fieldNames ===
//          "category" ::
//          "subcategory" ::
//          "header" ::
//          "page" ::
//          "uri" :: Nil
//    )
//
//    val formatted = result.toJSON.collect().mkString("\n")
//    assert(
//      formatted ===
//        """
//          |{"category":"Home"}
//          |{"category":"Computers","subcategory":"Laptops","header":"Computers / Laptops","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops"}
//          |{"category":"Computers","subcategory":"Tablets","header":"Computers / Tablets","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
//          |{"category":"Phones","subcategory":"Touch","header":"Phones / Touch","uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch"}
//          |{"category":"Computers","subcategory":"Laptops","header":"Computers / Laptops","page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/3"}
//          |{"category":"Computers","subcategory":"Laptops","header":"Computers / Laptops","page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/2"}
//          |{"category":"Computers","subcategory":"Laptops","header":"Computers / Laptops","page":"1","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/1"}
//          |{"category":"Computers","subcategory":"Tablets","header":"Computers / Tablets","page":"4","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
//          |{"category":"Computers","subcategory":"Tablets","header":"Computers / Tablets","page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
//          |{"category":"Computers","subcategory":"Tablets","header":"Computers / Tablets","page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
//          |{"category":"Computers","subcategory":"Tablets","header":"Computers / Tablets","page":"1","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1"}
//          |{"category":"Phones","subcategory":"Touch","header":"Phones / Touch","page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch/2"}
//          |{"category":"Phones","subcategory":"Touch","header":"Phones / Touch","page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch/1"}
//        """.stripMargin.trim
//    )
//  }
//
//  override def numPages = {
//    case Wide_WebCachedRDD => 15
//    case _ => 16
//  }
//
//  override def numDrivers = 0
//}
