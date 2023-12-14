package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.ForkType.Outer
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

/**
  * Created by peng on 12/10/14.
  */
class ForkAndExplorePagesIT extends ITBaseSpec {

  override lazy val driverFactories: Seq[Null] = Seq(
    null
  )

  override def doMain(): Unit = {

    val forked = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/static")
      )
      .fork(S"div.sidebar-nav a", Outer, ordinalField = 'i1)
      .fetch(
        Wget('A.href)
      )
      .extract(
        'A.text ~ 'category
      )
      .fork(S"a.subcategory-link", Outer, ordinalField = 'i2)
      .fetch(
        Wget('A.href)
      )
      .extract(
        'A.text ~ 'subcategory,
        S"h1".text ~ 'header
      )

    val result = forked
      .removeWeaks()
      .explore(S"ul.pagination a", ordinalField = 'i3)(
        Wget('A.href),
        depthField = 'depth
      )
      .extract(
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

    result.toJSON
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":0,"uri":"http://localhost:10092/test-sites/e-commerce/static/computers/laptops"}
        |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":1,"i3":[0],"page":"2","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/laptops/2"}
        |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":1,"i3":[1],"page":"3","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/laptops/3"}
        |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops","depth":2,"i3":[0,0],"page":"«","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/laptops/1"}
        |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":0,"uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets"}
        |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[0],"page":"2","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/2"}
        |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[1],"page":"3","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/3"}
        |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":1,"i3":[2],"page":"4","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/4"}
        |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets","depth":2,"i3":[0,0],"page":"«","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/1"}
        |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":0,"uri":"http://localhost:10092/test-sites/e-commerce/static/phones/touch"}
        |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":1,"i3":[0],"page":"2","uri":"http://localhost:10092/test-sites/e-commerce/static/phones/touch/2"}
        |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch","depth":2,"i3":[0,0],"page":"«","uri":"http://localhost:10092/test-sites/e-commerce/static/phones/touch/1"}
        """.stripMargin.trim
      )
  }

  override def numPages: Long = 15

  override val remoteFetchSuboptimality: Range = 0 to 4
  override def pageFetchedCap: Long = 40
}
