package com.tribbloids.spookystuff.integration.extract

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

/**
  * Created by peng on 11/26/14.
  */
class ChainForkExtractIT extends ITBaseSpec {

  override lazy val webDriverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val r1 = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/allinone") // this site is unstable, need to revise
      )
      .fork(S"div.thumbnail", ordinalField = 'i1)(
        A"p".attr("class") ~ 'p_class
      )

    val r2 = r1
      .fork(A"h4", ordinalField = 'i2)(
        'A.attr("class") ~ 'h4_class
      )

    val r3 = {
      r2
        .fork(
          S"notexist",
          ordinalField = 'notexist_key
        )( // this is added to ensure that temporary joinKey in KV store won't be used.
          'A.attr("class") ~ 'notexist_class
        )
    }

    val result = r3
      .toDF(sort = true)

    result.schema.treeString.shouldBe(
      """
        |root
        | |-- i1: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- p_class: string (nullable = true)
        | |-- i2: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- h4_class: string (nullable = true)
        | |-- notexist_key: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- notexist_class: string (nullable = true)
      """.stripMargin
    )

    result.toJSON
      .collect()
      .mkString("\n")
      .shouldBe(
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

  override def numPages: Long = 1
}
