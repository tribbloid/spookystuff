package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

class InterpolationSuite extends FunSpecx {

  it("interpolate can use common character as delimiter") {

    val original = "ORA'{TEST}"
    val interpolated = Interpolation.`'`(original) { _ =>
      "Replaced"
    }
    assert(interpolated == "ORAReplaced")
  }

  it("interpolate can use special regex character as delimiter") {

    val original = "ORA${TEST}"
    val interpolated = Interpolation.`$`(original) { _ =>
      "Replaced"
    }
    assert(interpolated == "ORAReplaced")
  }

  val special: Seq[String] = Seq(
    "$",
    "\\$",
    "\\\\$"
  )

  it("interpolate should ignore string that contains delimiter without bracket") {

    special.foreach { ss =>
      val original = s"ORA${ss}TEST"
      val interpolated = Interpolation.`$`(original) { _ =>
        "Replaced"
      }
      assert(interpolated == original)
    }

    special.foreach { ss =>
      val original = s"ORA$ss"
      val interpolated = Interpolation.`$`(original) { _ =>
        "Replaced"
      }
      assert(interpolated == original)
    }
  }

  it("interpolate should allow delimiter to be escaped") {

    special.foreach { ss =>
      val original = "ORA" + ss + "${TEST}"
      val interpolated = Interpolation.`$`(original) { _ =>
        "Replaced"
      }
      assert(interpolated == original.replaceAllLiterally("$$", "$"))
    }

    special.foreach { ss =>
      val original = s"ORA" + ss + "${}"
      val interpolated = Interpolation.`$`(original) { _ =>
        "Replaced"
      }
      assert(interpolated == original.replaceAllLiterally("$$", "$"))
    }
  }

  val i_$ : Interpolation = Interpolation.`$`

  it("can interpolate delimited field") {

    assert(
      i_$.apply("abc${def}ghi") { s =>
        s + "0"
      } == "abcdef0ghi"
    )
  }

  it("can avoid escaped char") {

    assert(
      i_$.apply("abc$${def}ghi") { s =>
        s + "0"
      } == "abc${def}ghi"
    )
  }

  it("can avoid continuous escaped chars") {
    assert(
      i_$.apply("abc$$$$$${def}ghi") { s =>
        s + "0"
      } == "abc$$${def}ghi"
    )
  }

  it("can identify delimiter after continuous escaped chars") {
    assert(
      i_$.apply("abc$$$$${def}ghi") { s =>
        s + "0"
      } == "abc$$def0ghi"
    )
  }
}
