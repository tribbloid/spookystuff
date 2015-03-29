package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/10/14.
 */
class ExplorePagesIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext): Unit = {
    import spooky._

    val result = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore($"ul.pagination a", depthKey = 'depth, ordinalKey = 'index)(
        Wget('A.href)
      )(
        'A.text ~ 'page
      )
      .select($.uri ~ 'uri)
      .toDataFrame()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "index" ::
          "page" ::
          "uri" :: Nil
    )

    val formatted = result.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"depth":0,"uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets"}
          |{"depth":1,"index":[0],"page":"2","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2"}
          |{"depth":1,"index":[1],"page":"3","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3"}
          |{"depth":1,"index":[2],"page":"4","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4"}
          |{"depth":2,"index":[0,0],"page":"«","uri":"http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1"}
        """.stripMargin.trim
    )
  }

  override def numPages = _ => 5

  override def numDrivers = _ => 0
}
