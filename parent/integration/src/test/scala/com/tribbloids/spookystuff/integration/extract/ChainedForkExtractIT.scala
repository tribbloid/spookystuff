package com.tribbloids.spookystuff.integration.extract

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

/**
  * Created by peng on 11/26/14.
  */
class ChainedForkExtractIT extends ITBaseSpec {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val r1 = spooky
      .fetch(
        Wget("http://localhost:10092/test-sites/e-commerce/allinone") // this site is unstable, need to revise
      )
      .fork(S"div.thumbnail", ordinalField = 'i1)
      .extract(
        A"p".attr("class") ~ 'p_class
      )
//    assert(r1.toDF().count() == 3)

    val r2 = r1
      .fork(A"h4", ordinalField = 'i2)
      .extract(
        'A.attr("class") ~ 'h4_class
      )
//    assert(r2.toDF().count() == 6)

    val result = r2
      .fork(
        S"notexist",
        forkType = ForkType.Outer,
        ordinalField = 'notexist_key
      )
      .extract( // this is added to ensure that temporary joinKey in KV store won't be used.
        'A.attr("class") ~ 'notexist_class
      )
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

    val formatted = result.toJSON.collect().toSeq
    assert(
      formatted ===
        """
          |{"i1":[0],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[0],"p_class":"description","i2":[1]}
          |{"i1":[1],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[1],"p_class":"description","i2":[1]}
          |{"i1":[2],"p_class":"description","i2":[0],"h4_class":"pull-right price"}
          |{"i1":[2],"p_class":"description","i2":[1]}
        """.stripMargin.trim.split('\n').toSeq
    )
  }

  override def numPages: Long = 1
}
