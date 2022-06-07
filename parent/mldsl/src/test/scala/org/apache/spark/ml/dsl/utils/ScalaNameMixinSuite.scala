package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

class ScalaNameMixinSuite extends FunSpecx {

  it("can process anonymous function dependent object") {

    object impl extends ScalaNameMixin
    def getImpl = {

      object impl extends ScalaNameMixin

      impl
    }

    val vs = (0 to 3).flatMap { _ =>
      Seq(impl, getImpl)
    }

    vs.foreach { v =>
      v.objectSimpleName.shouldBe(
        "impl"
      )
    }
  }
}
