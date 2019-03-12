package com.tribbloids.spookystuff.graph

import java.util.UUID

import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator

trait IDAlgebra[ID, -NodeData] {

  val DANGLING: ID

  val reserved = Set(DANGLING) // will be excluded from rotation

  final def retryUntilNotReserved(fn: => ID): ID = {

    for (i <- 1 to 10) {
      val result = fn
      if (!reserved.contains(result)) return result
    }
    sys.error("cannot find non-reserved ID")
  }

  protected def _random(): ID
  def init(v: NodeData): ID = retryUntilNotReserved {
    _random()
  }

  def rotatorFactory(): () => Rotator[ID]

}

object IDAlgebra {

  abstract class Rotator[ID](_reserved: Set[ID]) extends (ID => ID) {

    //TODO: need local caching?

    override final def apply(v: ID): ID = {
      if (_reserved.contains(v)) v
      else _doApply(v)
    }

    protected def _doApply(v: ID): ID
  }

  case object UUIDAlgebra extends IDAlgebra[UUID, Any] {

    override val DANGLING: UUID = UUID.fromString("dangling")

    override def _random(): UUID = UUID.randomUUID()

    override def rotatorFactory() = RotatorFactory()

    case class RotatorFactory() extends (() => RotatorImpl) {

      override def apply(): RotatorImpl = {
        RotatorImpl()
      }
    }

    case class RotatorImpl(seed: UUID = _random()) extends Rotator[UUID](reserved) {

      override def _doApply(v1: UUID): UUID = {
        val higher = v1.getMostSignificantBits ^ seed.getMostSignificantBits
        val lower = v1.getLeastSignificantBits ^ seed.getLeastSignificantBits
        new UUID(higher, lower)
      }
    }
  }
}
