package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.DataRowSchema

/**
  * Created by peng on 14/06/16.
  */
class SchemaContextSuite extends SpookyEnvFixture{

  import com.tribbloids.spookystuff.dsl._

  it("Resolver should not scramble sequence of fields") {

    val schema0 = DataRowSchema(ExecutionContext(spooky))
    val resolver0 = schema0.newResolver
    resolver0
      .include(
        Lit("literal1") ~ 'a,
        Lit(0) ~ 'b
      )

    val schema1 = resolver0.build
    schema1.toStructType.toString().shouldBe(
      "StructType(StructField(a,StringType,true), StructField(b,IntegerType,true))"
    )
    val resolver1 = schema1.newResolver
    resolver1
      .include(
        'a ~ 'c,
        Lit(0.0) ~ 'd
      )

    val schema2 = resolver1.build
    schema2.toStructType.toString().shouldBe(
      "StructType(StructField(a,StringType,true), StructField(b,IntegerType,true), StructField(c,StringType,true), StructField(d,DoubleType,true))"
    )
  }
}
