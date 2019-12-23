package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.sql.catalyst.ScalaReflection

class UnReifiedObjectTypeSuite extends FunSpecx {

  import ScalaReflection.universe._

  it("UnreifiedScalaType.toString") {

    val tt = UnreifiedObjectType.forType(typeTag[Int])
    tt.toString.shouldBe(
      "(unreified) ObjectType(int)"
    )
  }
}
