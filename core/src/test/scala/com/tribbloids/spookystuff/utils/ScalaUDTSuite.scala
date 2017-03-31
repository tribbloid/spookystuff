package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.{Doc, Fetched, Unstructured}
import com.tribbloids.spookystuff.testutils.FunSuitex
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 28/05/16.
  */
class ScalaUDTSuite extends SpookyEnvFixture with FunSuitex {

  import ScalaType._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def getAndTestReifiedType[T: TypeTag]: DataType = {
    val unreified = UnreifiedScalaType.apply[T]
    assertSerializable(unreified)

    val reified = TypeUtils.tryCatalystTypeFor[T].get
    assert(reified == unreified.reify)
    assertSerializable(reified)
    reified
  }

  test("Int has a datatype") {

    val reified = getAndTestReifiedType[Int]
    reified.toString.shouldBe(
      """
        |IntegerType
      """.stripMargin
    )
  }

  test("Array[Int] has a datatype") {

    val reified = getAndTestReifiedType[Array[Int]]
    reified.toString.shouldBe(
      """
        |ArrayType(IntegerType,false)
      """.stripMargin
    )
  }

  test("Action has a datatype") {

    val reified = getAndTestReifiedType[Action]

    val typeName = reified.typeName
    reified.toString.shouldBe(
      """
        |ActionUDT
      """.stripMargin
    )
  }

  test("Array[Action] has a datatype") {

    val reified = getAndTestReifiedType[Array[Action]]
    reified.toString.shouldBe(
      """
        |ArrayType(ActionUDT,true)
      """.stripMargin
    )
  }

  test("Unstructured has a datatype") {

    val reified = getAndTestReifiedType[Unstructured]
    reified.toString.shouldBe(
      """
        |UnstructuredUDT
      """.stripMargin
    )
  }

  test("Fetched has a datatype") {

    val reified = getAndTestReifiedType[Fetched]
    reified.toString.shouldBe(
      """
        |FetchedUDT
      """.stripMargin
    )
  }

  test("Doc has a datatype") {

    val reified = getAndTestReifiedType[Doc]
    reified.toString.shouldBe(
      """
        |UnstructuredUDT
      """.stripMargin
    )
  }
}