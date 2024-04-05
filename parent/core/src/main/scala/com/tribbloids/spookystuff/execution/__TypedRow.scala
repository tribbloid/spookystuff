package com.tribbloids.spookystuff.execution

object __TypedRow {

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

  /**
    * For explore, things are more complex, as it involves a 2-stage resolving process:
    *
    *   - apply Delta, commit into visited (I => O)
    *
    *   - apply Fork, commit into open (I => J => (I, Trace), such that fork can be recursively applied to I)
    *     - or (I => Forking[I, K] => (I, Trace))
    *
    * In the meantime, it is like a recursive fork (I => (I, Trace)) + select (I => O)
    *   - after __DefineByRun, it will be only 1 step: I => (O, Trace), why can't explore do the same?
    *
    * Can it use integrator pattern? Namely
    *
    * Either[I, O1] => O1
    *
    * the result of any of them can be used by the other
    */
}
