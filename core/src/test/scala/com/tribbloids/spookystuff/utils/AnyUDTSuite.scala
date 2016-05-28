package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.tests.TestMixin
import org.apache.spark.sql.TypeUtils
import org.scalatest.FunSuite

/**
  * Created by peng on 28/05/16.
  */
class AnyUDTSuite extends FunSuite with TestMixin {

  test("Action has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Action].get
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.actions.ActionUDT@3a195b03
      """.stripMargin
    )
  }

  test("Array[Action] has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Array[Action]].get
    schema.toString.shouldBe(
      """
        |ArrayType(com.tribbloids.spookystuff.actions.ActionUDT@3a195b03,true)
      """.stripMargin
    )
  }

  test("Fetched has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Fetched].get
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.doc.FetchedUDT@db1aece7
      """.stripMargin
    )
  }
}
