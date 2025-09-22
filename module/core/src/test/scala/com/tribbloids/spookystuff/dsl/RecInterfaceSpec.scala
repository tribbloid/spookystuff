package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.testutils.SpookyEnvSpec

class RecInterfaceSpec extends SpookyEnvSpec {

  describe("withColumnsMany") {

    it("record + record") {

      val v = spooky
        .select { _ =>
          Rec(a = 1)
        }
        .withColumnsMany { _ =>
          Seq(Rec(b = 2), Rec(b = 3))
        }

      val ds = v.asDataset
      ds.toJSON
        .collect()
        .mkString("\n")
        .shouldBe(
          """
          |{"a":1,"b":2}
          |{"a":1,"b":3}
          |""".stripMargin
        )
    }

    it("value + record") {

      val v = spooky
        .select { _ =>
          "x"
        }
        .withColumnsMany { _ =>
          Seq(Rec(b = 2), Rec(b = 3))
        }

      val ds = v.asDataset
      ds.toJSON
        .collect()
        .mkString("\n")
        .shouldBe(
          """
            |{"value":"x","b":2}
            |{"value":"x","b":3}
            |""".stripMargin
        )

    }

  }

  describe("withColumns") {

    it("record + record") {

      val v = spooky
        .select { _ =>
          Rec(a = 1)
        }
        .withColumns { _ =>
          Rec(b = 2, c = 3)
        }

      val ds = v.asDataset
      ds.toJSON
        .collect()
        .mkString("\n")
        .shouldBe(
          """
            |{"a":1,"b":2,"c":3}
            |""".stripMargin
        )
    }

    it("value + record") {

      val v = spooky
        .select { _ =>
          "x"
        }
        .withColumns { _ =>
          Rec(b = 2, c = 3)
        }

      val ds = v.asDataset
      ds.toJSON
        .collect()
        .mkString("\n")
        .shouldBe(
          """
            |{"value":"x","b":2,"c":3}
            |""".stripMargin
        )
    }

  }
}

object RecInterfaceSpec
