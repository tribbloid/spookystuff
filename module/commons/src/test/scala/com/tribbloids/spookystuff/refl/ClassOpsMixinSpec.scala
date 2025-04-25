package com.tribbloids.spookystuff.refl

import com.tribbloids.spookystuff.commons.refl.ClassOpsMixin
import com.tribbloids.spookystuff.testutils.BaseSpec

class ClassOpsMixinSpec extends BaseSpec {

  it("can process anonymous function dependent object") {

    object impl extends ClassOpsMixin
    def getImpl = {

      object impl extends ClassOpsMixin

      impl
    }

    val vs: Seq[ClassOpsMixin] = (0 to 3).flatMap { _ =>
      Seq(impl, getImpl)
    }

    vs.foreach { v =>
      val clz = v.getClass

      clz.simpleName_Scala.shouldBe(
        "impl"
      )
    }
  }
}
