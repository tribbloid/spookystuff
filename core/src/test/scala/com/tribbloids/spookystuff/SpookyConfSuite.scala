package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.testutils.TestMixin
import org.scalatest.FunSuite

/**
  * Created by peng on 30/09/16.
  */
class SpookyConfSuite extends SpookyEnvFixture {

  val conf = new SpookyConf()
  test("SpookyConf is serializable") {

    assertSerializable(
      conf,
      condition = {
        (v1: SpookyConf, v2: SpookyConf) =>
          v1.components.mkString("\n").shouldBe (
            v2.components.mkString("\n")
          )
      }
    )
  }

  test("SpookyConf.import is serializable") {
    val imported = conf.importFrom(sc)
    assertSerializable(
      imported,
      condition = {
        (v1: SpookyConf, v2: SpookyConf) =>
          v1.components.mkString("\n").shouldBe (
            v2.components.mkString("\n")
          )
      }
    )
  }
}
