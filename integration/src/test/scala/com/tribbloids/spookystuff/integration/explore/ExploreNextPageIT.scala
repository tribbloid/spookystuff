package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

/**
  * Created by peng on 12/10/14.
  */
class ExploreNextPageIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(): Unit = {

    val result = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore(S"ul.pagination a[rel=next]", ordinalField = 'index)(
        Wget('A.href),
        depthField = 'page
      )(
        'A.text ~ 'button_text,
        S.uri ~ 'uri
      )
      .toDF(sort = true)

    assert(
      result.schema.fieldNames ===
        "page" ::
          "index" ::
          "button_text" ::
          "uri" :: Nil
    )

    val formatted = result.toJSON.collect().toSeq
    assert(
      formatted ===
        """
          |{"page":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
          |{"page":1,"index":[0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
          |{"page":2,"index":[0,0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
          |{"page":3,"index":[0,0,0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages = 4

  override def numDrivers = 0
}
