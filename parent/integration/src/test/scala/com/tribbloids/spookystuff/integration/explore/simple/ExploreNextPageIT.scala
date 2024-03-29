package com.tribbloids.spookystuff.integration.explore.simple

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

/**
  * Created by peng on 12/10/14.
  */
class ExploreNextPageIT extends ITBaseSpec {

  override lazy val webDriverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val result = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/static/computers/tablets")
      )
      .explore(S"ul.pagination a[rel=next]", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'page
      )
      .extract(
        'A.text ~ 'button_text,
        S.uri ~ 'uri
      )
      .toDF(sort = true)

    result.schema.treeString.shouldBe(
      """
        |root
        | |-- page: integer (nullable = true)
        | |-- index: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- button_text: string (nullable = true)
        | |-- uri: string (nullable = true)
      """.stripMargin
    )

    val formatted = result.toJSON
      .collect()
      .toSeq
      .mkString("\n")
      .shouldBe(
        """
        |{"page":0,"uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets"}
        |{"page":1,"index":[0],"button_text":"»","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/2"}
        |{"page":2,"index":[0,0],"button_text":"»","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/3"}
        |{"page":3,"index":[0,0,0],"button_text":"»","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/4"}
        """.stripMargin.trim
      )
  }

  override def numPages: Long = 4
}
