package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationFixture

class ExplorePagesIT extends IntegrationFixture {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    //    spooky.conf.defaultPartitionerFactory = {v => new HashPartitioner(1)}

    val result = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore(S"ul.pagination a", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'depth
      )(
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

    val formatted = result.toJSON.collect().toSeq
    assert(
      formatted ===
        """
          |{"depth":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
          |{"depth":1,"index":[0],"page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
          |{"depth":1,"index":[1],"page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
          |{"depth":1,"index":[2],"page":"4","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
          |{"depth":2,"index":[0,0],"page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1"}
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages= 5
  override val error = 0 to 3

  override def pageFetchedCap: Int = 13
}
