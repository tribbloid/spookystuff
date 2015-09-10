package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Visit
import com.tribbloids.spookystuff.dsl._

/**
 * Created by peng on 12/5/14.
 */
class InnerJoinIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    DriverFactories.PhantomJS() //TODO: HtmlUnit does not support Backbone.js
  )

  override def doMain(spooky: SpookyContext): Unit = {

    import spooky.dsl._

    val base = spooky
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join(S"div.sidebar-nav a", ordinalKey = 'i1)(
        Visit('A.href),
        joinType = Inner,
        flattenPagesOrdinalKey = 'page
      )(
        'A.text ~ 'category
      )
      .join(S"a.subcategory-link", ordinalKey = 'i2)(
        Visit('A.href),
        joinType = Inner
      )(
        'A.text ~ 'subcategory
      )
      .select(S"h1".text ~ 'header)
      .flatSelect(S"notexist", ordinalKey = 'notexist_key)( //this is added to ensure that temporary joinKey in KV store won't be used.
        'A.attr("class") ~ 'notexist_class
      )
      .toDF(sort = true)

    assert(
      joined.schema.fieldNames ===
        "i1" ::
          "page" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" ::
          "notexist_key" ::
          "notexist_class" ::
          Nil
    )

    val formatted = joined.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"i1":[1],"page":[0],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops"}
          |{"i1":[1],"page":[0],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets"}
          |{"i1":[2],"page":[0],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch"}
        """.stripMargin.trim
    )
  }

  override def numPages = {
    case Wide_RDDWebCache => 6
    case _ => 7
  }
}
