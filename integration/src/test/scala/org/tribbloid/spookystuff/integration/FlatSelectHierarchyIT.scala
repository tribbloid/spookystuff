package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
* Created by peng on 11/26/14.
*/
class FlatSelectHierarchyIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    import spooky.dsl._

    val result = spooky
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone") //this site is unstable, need to revise
      )
      .flatSelect(S"div.thumbnail", ordinalKey = 'i1)(
        A"p".attr("class") ~ 'p_class
      )
      .flatSelect(A"h4", ordinalKey = 'i2)(
        'A.attr("class") ~ 'h4_class
      )
      .toDF(sort = true)

    assert(
      result.schema.fieldNames ===
        "i1" ::
          "p_class" ::
          "i2" ::
          "h4_class" :: Nil
    )

    val formatted = result.toJSON.collect().mkString("\n")
    assert(
      formatted ===
        """
          |{"i1":[0],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[0],"p_class":"description","i2":[1]}
          |{"i1":[1],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[1],"p_class":"description","i2":[1]}
          |{"i1":[2],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[2],"p_class":"description","i2":[1]}
        """.stripMargin.trim
    )
  }

  override def numPages = _ => 1

  override def numDrivers = 0
}