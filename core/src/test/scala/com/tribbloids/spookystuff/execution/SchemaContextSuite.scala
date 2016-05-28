package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.extractors.Literal

/**
  * Created by peng on 14/06/16.
  */
class SchemaContextSuite extends SpookyEnvSuite{

  import com.tribbloids.spookystuff.dsl._

  test("Resolver should not scramble sequence of fields") {

    val schema0 = SchemaContext(spooky)
    val resolver0 = schema0.newResolver
    resolver0
      .resolve(
        "literal1" ~ 'a,
        Literal(0) ~ 'b
      )

    val schema1 = resolver0.build
    schema1.toStructType().toString().shouldBe(
      "StructType(StructField(a,StringType,true), StructField(b,IntegerType,true))"
    )
    val resolver1 = schema1.newResolver
    resolver1
      .resolve(
        'a ~ 'c,
        Literal(0.0) ~ 'd
      )

    val schema2 = resolver1.build
    schema2.toStructType().toString().shouldBe(
      "StructType(StructField(a,StringType,true), StructField(b,IntegerType,true), StructField(c,StringType,true), StructField(d,DoubleType,true))"
    )
  }
}
