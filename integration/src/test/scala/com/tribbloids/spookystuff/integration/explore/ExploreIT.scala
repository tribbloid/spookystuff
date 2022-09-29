package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationFixture

/**
  * Created by peng on 12/5/14.
  */
class ExploreIT extends IntegrationFixture {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val base = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/allinone")
      )

    val explored = base
      .explore(S"div.sidebar-nav a", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'depth
      )(
        'A.text ~ 'category,
        S"h1".text ~ 'header,
        S"notexist" ~ 'A.*
      )
      .flatSelect(
        'A,
        ordinalField = 'notexist_key
      )( // this is added to ensure that temporary joinKey in KV store won't be used.
        'A.attr("class") ~ 'notexist_class
      )
      .toDF(sort = true)

    explored.schema.treeString.shouldBe(
      """
        |root
        | |-- depth: integer (nullable = true)
        | |-- index: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- category: string (nullable = true)
        | |-- header: string (nullable = true)
        | |-- notexist_key: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- notexist_class: string (nullable = true)
      """.stripMargin
    )

    val formatted = explored.toJSON.collect().toSeq
    assert(
      formatted ===
        """
          |{"depth":0,"header":"E-commerce training site"}
          |{"depth":1,"index":[1],"category":"Computers","header":"Computers category"}
          |{"depth":1,"index":[2],"category":"Phones","header":"Phones category"}
          |{"depth":2,"index":[1,2],"category":"Laptops","header":"Computers / Laptops"}
          |{"depth":2,"index":[1,3],"category":"Tablets","header":"Computers / Tablets"}
          |{"depth":2,"index":[2,3],"category":"Touch","header":"Phones / Touch"}
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages = 6
  override def pageFetchedCap = 18 // way too large
}
