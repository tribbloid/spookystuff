package com.tribbloids.spookystuff.integration.fork

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.dsl.ForkType.Inner
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.integration.ITBaseSpec
import com.tribbloids.spookystuff.web.actions.Visit

class VisitInnerForkIT extends ITBaseSpec {

//  override lazy val driverFactories = Seq(
//    phantomJS //TODO: HtmlUnit does not support Backbone.js
//  )

  def getPage(uri: Col[String]): Action = Visit(uri)

  override def doMain(): Unit = {

    val base = spooky
      .fetch(
        getPage("http://localhost:10092/test-sites/e-commerce/allinone")
      )
      .savePages_!("~/temp/abc")

    val joined = base
      .fork(S"div.sidebar-nav a", Inner, ordinalField = 'i1)
      .fetch(
        getPage('A.href)
      )(
        G ~+ 'page,
        'A.text ~ 'category
      )
      .fork(S"a.subcategory-link", Inner, ordinalField = 'i2)
      .fetch(
        getPage('A.href)
      )(
        'A.text ~ 'subcategory
      )
      .extract(S"h1".text ~ 'header)
      .fork(
        S"notexist",
        ordinalField = 'notexist_key
      )( // this is added to ensure that temporary joinKey in KV store won't be used.
        'A.attr("class") ~ 'notexist_class
      )

    val df = joined
      .toDF(sort = true)

    df.schema.treeString.shouldBe(
      """
        |root
        | |-- i1: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- page: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- category: string (nullable = true)
        | |-- i2: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- subcategory: string (nullable = true)
        | |-- header: string (nullable = true)
        | |-- notexist_key: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- notexist_class: string (nullable = true)
      """.stripMargin
    )

    val formatted = df.toJSON.collect().mkString("\n")
    formatted.shouldBe(
      """
        |{"i1":[1],"page":[0],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops"}
        |{"i1":[1],"page":[0],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets"}
        |{"i1":[2],"page":[0],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch"}
      """.stripMargin
    )
  }

  override def numPages: Long = spooky.conf.localityPartitioner match {
    //    case FetchOptimizers.WebCacheAware => 6
    case _ => 6
  }
}
