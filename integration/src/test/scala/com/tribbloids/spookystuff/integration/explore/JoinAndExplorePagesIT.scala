package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationFixture

/**
  * Created by peng on 12/10/14.
  */
class JoinAndExplorePagesIT extends IntegrationFixture {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val joined = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static")
      )
      .join(S"div.sidebar-nav a", LeftOuter, ordinalField = 'i1)(
        Wget('A.href)
      )
      .extract(
        'A.text ~ 'category
      )
      .join(S"a.subcategory-link", LeftOuter, ordinalField = 'i2)(
        Wget('A.href)
      )
      .extract(
        'A.text ~ 'subcategory,
        S"h1".text ~ 'header
      )

    val result = joined
      .removeWeaks()
      .explore(S"ul.pagination a", ordinalField = 'i3)(
        Wget('A.href),
        depthField = 'depth
      )(
        'A.text as 'page,
        S.uri ~ 'uri
      )
      .toDF(sort = true)
      .persist()

    result.schema.treeString.shouldBe(
"""
  |root
  | |-- i1: array (nullable = true)
  | |    |-- element: integer (containsNull = true)
  | |-- category: string (nullable = true)
  | |-- i2: array (nullable = true)
  | |    |-- element: integer (containsNull = true)
  | |-- subcategory: string (nullable = true)
  | |-- header: string (nullable = true)
  | |-- depth: integer (nullable = true)
  | |-- i3: array (nullable = true)
  | |    |-- element: integer (containsNull = true)
  | |-- page: string (nullable = true)
  | |-- uri: string (nullable = true)
""".stripMargin
    )

    val formatted = result.toJSON.collect().toSeq
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
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages= 15

  override val remoteFetchSuboptimality = 0 to 4
  override def pageFetchedCap = 40
}
