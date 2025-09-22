package com.tribbloids.spookystuff.integration.explore.simple

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

class ExplorePagesIT extends ITBaseSpec {

  override lazy val webDriverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    //    spooky.conf.defaultPartitionerFactory = {v => new HashPartitioner(1)}

    val result = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/static/computers/tablets")
      )
      .explore(S"ul.pagination a", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'depth
      )
      .extract(
        'A.text ~ 'page,
        S.uri ~ 'uri
      )
      .toDF(sort = true)

    result.schema.treeString.shouldBe(
      """
        |root
        | |-- depth: integer (nullable = true)
        | |-- index: array (nullable = true)
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
        |{"depth":0,"uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets"}
        |{"depth":1,"index":[0],"page":"2","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/2"}
        |{"depth":1,"index":[1],"page":"3","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/3"}
        |{"depth":1,"index":[2],"page":"4","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/4"}
        |{"depth":2,"index":[0,0],"page":"«","uri":"http://localhost:10092/test-sites/e-commerce/static/computers/tablets/1"}
        """.stripMargin.trim
      )
  }

  override def numPages: Long = 5
  override val maxRedundantFetch: Range.Inclusive = 0 to 3

  override def pageFetchedCap: Long = 14 // TODO: this should be smaller, at least 13
}
