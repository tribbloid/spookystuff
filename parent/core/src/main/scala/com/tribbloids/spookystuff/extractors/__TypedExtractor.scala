package com.tribbloids.spookystuff.extractors

object __TypedExtractor {

  /**
    * This rewrite could introduce the following capabilities, and drastically simplify extractor impl (by eliminating
    * the need for 2-stage resolving)
    *
    *   - Typed and Columnar [[com.tribbloids.spookystuff.row.Lineage]]
    *   - A NamedTuple API that can be smoothly transformed to Scala 3's canonical implementation in the future
    *   - Better frameless interoperability
    *   - more fluent API for custom extractors
    *
    * CAUTION: it should spend no effort on representing row with frameless or
    * [[org.apache.spark.sql.catalyst.InternalRow]], or accelerating extraction with Catalyst/Tungsten/CodeGen, these
    * optimisations are still desirable and should be done later
    *
    * After this rewrite, the project will reach a bottleneck suitable for publishing under a different groupId &
    * package name
    */
}
