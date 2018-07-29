package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.{Doc, DocOption, Unstructured}
import com.tribbloids.spookystuff.testutils.{AssertSerializable, FunSpecx}
import org.apache.spark.ml.dsl.utils.refl.{TypeUtils, UnreifiedScalaType}
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 28/05/16.
  */
class ScalaUDTSuite extends SpookyEnvFixture with FunSpecx {

  import org.apache.spark.ml.dsl.utils.refl.ScalaType._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def getAndTestReifiedType[T: TypeTag]: DataType = {
    val unreified = UnreifiedScalaType.forType[T]
    AssertSerializable(unreified)

    val reified = TypeUtils.tryCatalystTypeFor[T].get
    assert(reified == unreified.reified)
    AssertSerializable(reified)
    reified
  }

  it("Action has a datatype") {

    val reified = getAndTestReifiedType[Action]

    val typeName = reified.typeName
    reified.toString.shouldBe(
      """
        |ActionUDT
      """.stripMargin
    )
  }

  it("Array[Action] has a datatype") {

    val reified = getAndTestReifiedType[Array[Action]]
    reified.toString.shouldBe(
      """
        |ArrayType(ActionUDT,true)
      """.stripMargin
    )
  }

  it("Unstructured has a datatype") {

    val reified = getAndTestReifiedType[Unstructured]
    reified.toString.shouldBe(
      """
        |UnstructuredUDT
      """.stripMargin
    )
  }

  it("DocOption has a datatype") {

    val reified = getAndTestReifiedType[DocOption]
    reified.toString.shouldBe(
      """
        |DocOptionUDT
      """.stripMargin
    )
  }

  it("Doc has a datatype") {

    val reified = getAndTestReifiedType[Doc]
    reified.toString.shouldBe(
      """
        |UnstructuredUDT
      """.stripMargin
    )
  }
}