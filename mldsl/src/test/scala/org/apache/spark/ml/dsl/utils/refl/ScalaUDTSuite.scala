package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import com.tribbloids.spookystuff.utils.AssertSerializable
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 28/05/16.
  */

class ScalaUDTSuite extends FunSpecx {

  TestHelper.TestSC

  import org.apache.spark.ml.dsl.utils.refl.ScalaType._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def getAndTestReifiedType[T: TypeTag]: DataType = {
    val unreified: DataType = UnreifiedScalaType.forType[T]
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

}