package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/5/14.
 */
class LeftJoinIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    DriverFactories.PhantomJS() //TODO: HtmlUnit does not support Backbone.js
  )

  override def doMain(spooky: SpookyContext): Unit = {

    val base = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join($"div.sidebar-nav a", ordinalKey = 'i1)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'category
      )
      .join($"a.subcategory-link", ordinalKey = 'i2)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'subcategory
      )
      .select($"h1".text ~ 'header)
      .toDF(sort = true)

    assert(
      joined.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" :: Nil
    )

    val formatted = joined.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"i1":[0],"category":"Home"}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets"}
          |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch"}
        """.stripMargin.trim
    )
  }

  override def numPages = {
    case Wide_WebCachedRDD => 6
    case _ => 7
  }

  override def numDrivers = 0
}
