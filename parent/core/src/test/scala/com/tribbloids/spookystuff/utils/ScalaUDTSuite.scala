package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.{Observation, Unstructured}
import com.tribbloids.spookystuff.testutils.{BaseSpec, SpookyBaseSpec}
import com.tribbloids.spookystuff.utils.refl.{CatalystTypeOps, TypeUtils, UnreifiedObjectType}
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 28/05/16.
  */
class ScalaUDTSuite extends SpookyBaseSpec with BaseSpec with CatalystTypeOps.ImplicitMixin {

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def getAndTestReifiedType[T: TypeTag]: DataType = {
    val unreified = UnreifiedObjectType.summon[T]
    AssertSerializable(unreified)

    val reified = TypeUtils.tryCatalystTypeFor[T].get
    assert(reified == unreified.reified)
    AssertSerializable(reified)
    reified
  }

  it("Int has a datatype") {

    val reified = getAndTestReifiedType[Int]
    reified.toString.shouldBe(
      """
        |IntegerType
      """.stripMargin
    )
  }

  it("Array[Int] has a datatype") {

    val reified = getAndTestReifiedType[Array[Int]]
    reified.toString.shouldBe(
      """
        |ArrayType(IntegerType,false)
      """.stripMargin
    )
  }

  it("Action has a datatype") {

    val reified = getAndTestReifiedType[Action]

    reified.typeName
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

    val reified = getAndTestReifiedType[Observation]
    reified.toString.shouldBe(
      """
        |FetchedUDT
      """.stripMargin
    )
  }

  // TODO: not anymore
//  it("Doc has a datatype") {
//
//    val reified = getAndTestReifiedType[Doc]
//    reified.toString.shouldBe(
//      """
//        |UnstructuredUDT
//      """.stripMargin
//    )
//  }
}
