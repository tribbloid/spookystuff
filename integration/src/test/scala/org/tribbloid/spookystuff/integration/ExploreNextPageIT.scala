package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/10/14.
 */
class ExploreNextPageIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext): Unit = {

    val result = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore(S"ul.pagination a[rel=next]", depthKey = 'page, ordinalKey = 'index)(
        Wget('A.href)
      )(
        'A.text ~ 'button_text
      )
      .select(S.uri ~ 'uri)
      .toDF(sort = true)

    assert(
      result.schema.fieldNames ===
        "page" ::
          "index" ::
          "button_text" ::
          "uri" :: Nil
    )

    val formatted = result.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"page":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
          |{"page":1,"index":[0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
          |{"page":2,"index":[0,0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
          |{"page":3,"index":[0,0,0],"button_text":"»","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
        """.stripMargin.trim
    )
  }

  override def numPages = _ => 4

  override def numDrivers = 0
}
