package ai.acyclic.prover.commons.pending

trait PendingEffect {}

object PendingEffect extends PendingGroup {

  //  trait Must[-S]

  // DO NOT CHANGE! will be delegated to kyo in Scala 3
  type <<[T, -S <: PendingEffect] >: T // | Must[S]
  // S is contravariant because Pending can be added implicitly

  trait Universe extends PendingGroup {}
}
