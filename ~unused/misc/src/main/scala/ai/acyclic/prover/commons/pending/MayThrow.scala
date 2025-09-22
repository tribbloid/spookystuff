package ai.acyclic.prover.commons.pending

import ai.acyclic.prover.commons.pending.PendingEffect.<<

import scala.reflect.ClassTag

object MayThrow {

  case class ^[E <: Throwable]() extends PendingEffect

  def apply[E <: Throwable] = new ^[E]()

  implicit class _ops[T, E <: Throwable](self: (() => T) << ^[E])(
      implicit
      ctg: ClassTag[E]
  ) {

    def asEither: Either[E, T] = {
      try {
        val v = self.asInstanceOf[() => T].apply()
        Right(v)
      } catch {
        case e: E => Left(e)
      }
    }
  }
}
