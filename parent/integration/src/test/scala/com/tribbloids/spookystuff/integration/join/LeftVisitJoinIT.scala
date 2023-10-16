package com.tribbloids.spookystuff.integration.join

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.JoinType.LeftOuter
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.integration.ITBaseSpec
import com.tribbloids.spookystuff.web.actions.Visit

/**
  * Created by peng on 12/5/14.
  */
class LeftVisitJoinIT extends ITBaseSpec {

  def getPage(uri: Col[String]): Action = Visit(uri)

  override def doMain(): Unit = {

    val base = spooky
      .fetch(
        getPage("http://localhost:10092/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join(S"div.sidebar-nav a", LeftOuter, ordinalField = 'i1)(
        getPage('A.href)
      )
      .extract(
        'A.text ~ 'category
      )
      .join(S"a.subcategory-link", LeftOuter, ordinalField = 'i2)(
        getPage('A.href)
      )
      .extract(
        'A.text ~ 'subcategory
      )
      .select(S"h1".text ~ 'header)
      .flatSelect(
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

    val formatted = df.toJSON.collect().toSeq
    assert(
      formatted ===
        """
          |{"i1":[0],"category":"Home"}
          |{"i1":[1],"category":"Computers","i2":[0],"subcategory":"Laptops","header":"Computers / Laptops"}
          |{"i1":[1],"category":"Computers","i2":[1],"subcategory":"Tablets","header":"Computers / Tablets"}
          |{"i1":[2],"category":"Phones","i2":[0],"subcategory":"Touch","header":"Phones / Touch"}
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages: Long = spooky.spookyConf.defaultGenPartitioner match {
    //    case FetchOptimizers.WebCacheAware => 6
    case _ => 6
  }
}
