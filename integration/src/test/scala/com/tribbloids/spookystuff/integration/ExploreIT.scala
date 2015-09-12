package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl._

/**
 * Created by peng on 12/5/14.
 */
class ExploreIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    DriverFactories.PhantomJS() //TODO: HtmlUnit does not support Backbone.js
  )

  override def doMain(spooky: SpookyContext): Unit = {

    val base = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val explored = base
      .explore(S"div.sidebar-nav a", depthKey = 'depth, ordinalKey = 'index)(
        Wget('A.href),
        flattenPagesOrdinalKey = 'page
      )(
        'A.text ~ 'category
      )
      .select(S"h1".text ~ 'header)
      .flatSelect(S"notexist", ordinalKey = 'notexist_key)( //this is added to ensure that temporary joinKey in KV store won't be used.
        'A.attr("class") ~ 'notexist_class
      )
      .toDF(sort = true)

    assert(
      explored.schema.fieldNames ===
        "depth" ::
          "index" ::
          "page" ::
          "category" ::
          "header" ::
          "notexist_key" ::
          "notexist_class" ::
          Nil
    )

    val formatted = explored.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"depth":0,"header":"E-commerce training site"}
          |{"depth":1,"index":[1],"page":[0],"category":"Computers","header":"Computers category"}
          |{"depth":1,"index":[2],"page":[0],"category":"Phones","header":"Phones category"}
          |{"depth":2,"index":[1,2],"page":[0,0],"category":"Laptops","header":"Computers / Laptops"}
          |{"depth":2,"index":[1,3],"page":[0,0],"category":"Tablets","header":"Computers / Tablets"}
          |{"depth":2,"index":[2,3],"page":[0,0],"category":"Touch","header":"Phones / Touch"}
        """.stripMargin.trim
    )
  }

  override def numFetchedPages = _ => 6

  override def numDrivers = 0
}