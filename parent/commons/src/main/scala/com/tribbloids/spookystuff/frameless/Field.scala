package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import ai.acyclic.prover.commons.util.Capabilities

object Field extends Capabilities {
  // ... is a compile-time-only construct

  trait CanSort extends Cap

  object CanSort extends Factory[CanSort] {

    object Enable extends Hom.Poly with Factory[CanSort] {

      implicit def only[T]: T =>> (T ^^ CanSort) = at[T] { v =>
        v.asInstanceOf[T ^^ CanSort]
      }
    }
  }

  /**
    * define whether to evict old values that has identical field name in previous table
    */
  sealed abstract class ConflictResolving extends Cap
  // TODO: it is useless right now, precedence of fields in merging are totally determined by whether to use ++< or >++
  //  may be enabled later for fine-grained control

  // Fail fast
  trait Error extends ConflictResolving

  // Always evict old value
  trait Replace extends ConflictResolving

  // Only evict old value if the new value is not NULL.
  trait ReplaceIfNotNull extends ConflictResolving
}
