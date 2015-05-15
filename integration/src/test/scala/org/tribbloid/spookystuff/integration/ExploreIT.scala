package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.Visit
import org.tribbloid.spookystuff.dsl._

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
        Visit("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val explored = base
      .explore($"div.sidebar-nav a", depthKey = 'depth, ordinalKey = 'index)(
        Visit('A.href),
        flattenPagesOrdinalKey = 'page
      )(
        'A.text ~ 'category
      )
      .select($"h1".text ~ 'header)
      .toDF(sort = true)

    assert(
      explored.schema.fieldNames ===
        "depth" ::
          "index" ::
          "page" ::
          "category" ::
          "header" :: Nil
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

  override def numPages = _ => 6
}