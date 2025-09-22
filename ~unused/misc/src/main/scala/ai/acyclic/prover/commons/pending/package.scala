package ai.acyclic.prover.commons

import ai.acyclic.prover.commons.pending.PendingEffect.<<

package object pending {

  final type Maybe[T <: Any] = T << Maybe.type

//  implicitly[Maybe[String] =:= String << Maybe.type]

  final type MayThrow[T, E <: Throwable] = (() => T) << MayThrow.^[E]
}
