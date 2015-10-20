package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

/**
 * Created by peng on 12/10/14.
 */
class JoinAndExplorePagesIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext): Unit = {

    val joined = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static")
      )
      .join(S"div.sidebar-nav a", ordinalKey = 'i1)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'category
      )
      .join(S"a.subcategory-link", ordinalKey = 'i2)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'subcategory
      )
      .select(S"h1".text ~ 'header)

    val result = joined
      .explore(S"ul.pagination a", depthKey = 'depth, ordinalKey = 'i3)(
        Wget('A.href)
      )(
        'A.text as 'page
      )
      .select(S.uri ~ 'uri)
      .toDF(sort = true)
      .persist()

    assert(
      result.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" ::
          "depth" ::
          "i3" ::
          "page" ::
          "uri" :: Nil
    )

    val formatted = result.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"i1":[0],"category":"Home","depth":0}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops"}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":1,"i3":[0],"page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/2"}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":1,"i3":[1],"page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/3"}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":2,"i3":[0,0],"page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/laptops/1"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[0],"page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[1],"page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[2],"page":"4","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":2,"i3":[0,0],"page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1"}
          |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch"}
          |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":1,"i3":[0],"page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch/2"}
          |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":2,"i3":[0,0],"page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/phones/touch/1"}
        """.stripMargin.trim
    )
  }

  override def numFetchedPages = {
    case Wide_RDDWebCache => 15
    case _ => 16
  }

  override def numDrivers = 0
}
