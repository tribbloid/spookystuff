package com.tribbloids.spookystuff.utils.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.utils.refl.UnreifiedObjectType
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
