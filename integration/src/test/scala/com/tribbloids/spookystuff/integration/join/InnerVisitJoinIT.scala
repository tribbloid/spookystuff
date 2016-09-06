package com.tribbloids.spookystuff.integration.join

import com.tribbloids.spookystuff.actions.{Action, Visit}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.integration.IntegrationFixture

class InnerVisitJoinIT extends IntegrationFixture {

//  override lazy val driverFactories = Seq(
//    phantomJS //TODO: HtmlUnit does not support Backbone.js
//  )

  def getPage(uri: Extractor[String]): Action = Visit(uri)

  override def doMain(): Unit = {

    val base = spooky
      .fetch(
        getPage("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join(S"div.sidebar-nav a", Inner, ordinalField = 'i1)(
        getPage('A.href)
      )
      .extract(
        G ~+ 'page,
        'A.text ~ 'category
      )
      .join(S"a.subcategory-link", Inner, ordinalField = 'i2)(
        getPage('A.href)
      )
      .extract(
        'A.text ~ 'subcategory
      )
      .extract(S"h1".text ~ 'header)
      .flatSelect(S"notexist", ordinalField = 'notexist_key)( //this is added to ensure that temporary joinKey in KV store won't be used.
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

  override def numPages= spooky.conf.defaultFetchOptimizer match {
    //    case FetchOptimizers.WebCacheAware => 6
    case _ => 6
  }
}

