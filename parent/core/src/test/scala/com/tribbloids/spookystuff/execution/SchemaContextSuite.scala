package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.testutils.SpookyEnvFixture

/**
  * Created by peng on 14/06/16.
  */
class SchemaContextSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  it("Resolver should not scramble sequence of fields") {

    val schema0 = SpookySchema(SpookyExecutionContext(spooky))
    val resolver0 = schema0.newResolver
    resolver0
      .include(
        Lit("literal1") ~ 'a,
        Lit(0) ~ 'b
      )

    val schema1 = resolver0.build
    schema1.structType.treeString
      .shouldBe(
        """
          |root
          | |-- a: string (nullable = true)
          | |-- b: integer (nullable = true)
          |""".stripMargin
      )
    val resolver1 = schema1.newResolver
    resolver1
      .include(
        'a ~ 'c,
        Lit(0.0) ~ 'd
      )

    val schema2 = resolver1.build
    schema2.structType.treeString
      .shouldBe(
        """
          |root
          | |-- a: string (nullable = true)
          | |-- b: integer (nullable = true)
          | |-- c: string (nullable = true)
          | |-- d: double (nullable = true)
          |""".stripMargin
      )
  }
}
