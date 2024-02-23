package com.tribbloids.spookystuff.commons.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.spark.sql.catalyst.ScalaReflection

class UnreifiedObjectTypeSuite extends BaseSpec {

  import ScalaReflection.universe._

  it("toString") {

    val tt = UnreifiedObjectType.summon(typeTag[Int])
    tt.toString.shouldBe(
      "(unreified) ObjectType(int)"
    )
  }
}
