package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.spark.sql.catalyst.ScalaReflection

class UnReifiedObjectTypeSuite extends BaseSpec {

  import ScalaReflection.universe._

  it("toString") {

    val tt = UnreifiedObjectType.summon(typeTag[Int])
    tt.toString.shouldBe(
      "(unreified) ObjectType(int)"
    )
  }
}
