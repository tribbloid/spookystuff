package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class FlatSelectHierarchyIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext) {

    import spooky._

    val result = noInput
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone")
      )
      .flatSelect($"div.thumbnail", ordinalKey = 'i1)(
        A"p.description" ~ 'description
      )
      .flatSelect(A"h4", ordinalKey = 'i2)(
        'A.text ~ 'name_or_price
      )
      .toSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "i1" ::
          "description" ::
          "i2" ::
          "name_or_price" :: Nil
    )

    val formatted = result.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"i1":[0],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[0],"name_or_price":"$109.99"}
          |{"i1":[0],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[1],"name_or_price":"Nokia X"}
          |{"i1":[1],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[0],"name_or_price":"$102.99"}
          |{"i1":[1],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[1],"name_or_price":"Asus MeMO Pad"}
          |{"i1":[2],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[0],"name_or_price":"$489.99"}
          |{"i1":[2],"description":[{"uri":"http://webscraper.io/test-sites/e-commerce/allinone"}],"i2":[1],"name_or_price":"Galaxy Note"}
        """.stripMargin.trim
    )
  }

  override def numPages = _ => 1

  override def numDrivers = _ => 0
}