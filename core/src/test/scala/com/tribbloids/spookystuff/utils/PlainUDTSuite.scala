package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.{Doc, Fetched, Unstructured}
import com.tribbloids.spookystuff.tests.TestMixin
import org.apache.spark.sql.TypeUtils
import org.scalatest.FunSuite

/**
  * Created by peng on 28/05/16.
  */
class PlainUDTSuite extends FunSuite with TestMixin {

  test("Action has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Action]
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.actions.ActionUDT@3a195b03
      """.stripMargin
    )
  }

  test("Array[Action] has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Array[Action]]
    schema.toString.shouldBe(
      """
        |ArrayType(com.tribbloids.spookystuff.actions.ActionUDT@3a195b03,true)
      """.stripMargin
    )
  }

  test("Unstructured has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Unstructured]
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.doc.UnstructuredUDT@3cd11d5c
      """.stripMargin
    )
  }

  test("Fetched has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Fetched]
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.doc.FetchedUDT@db1aece7
      """.stripMargin
    )
  }

  test("Doc has a datatype") {

    val schema = TypeUtils.catalystTypeFor[Doc]
    schema.toString.shouldBe(
      """
        |com.tribbloids.spookystuff.doc.DocUDT@6f45c46
      """.stripMargin
    )
  }
}