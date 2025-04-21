package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.debug.print_@
import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.testutils.SpookyEnvSpec

class RecInterfaceSpec extends SpookyEnvSpec {

  import DataViewSpec.resource.*
  import Rec.*

  describe("withColumnsMany") {

    it("record + record") {

      val v = spooky
        .select { row =>
          Rec(a = 1)
        }
        .withColumnsMany { row =>
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
        .select { row =>
          "x"
        }
        .withColumnsMany { row =>
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
        .select { row =>
          Rec(a = 1)
        }
        .withColumns { row =>
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
        .select { row =>
          "x"
        }
        .withColumns { row =>
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
