package ai.acyclic.prover.commons.pending

import ai.acyclic.prover.commons.pending.PendingEffect.<<

object Maybe extends PendingEffect {

  def unset[T]: T << Maybe.type = null.asInstanceOf[T << Maybe.type]

  implicit class _ops[T <: Any](self: Maybe[T]) {

//    implicitly[self.type <:< ((T << Maybe.type) << Maybe.type)]

    def asOption: Option[T] = self match {
      case null => None
      case v    => Some(Maybe.revoke[T](v))
    }
  }

//  implicit def _ops2[T](v: T << Maybe.type): _ops[T] = new _ops(v)
}
