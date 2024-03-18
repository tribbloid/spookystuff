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

}
