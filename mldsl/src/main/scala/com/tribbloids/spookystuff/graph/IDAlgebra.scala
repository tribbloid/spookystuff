package com.tribbloids.spookystuff.graph

import java.util.UUID

import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator

trait IDAlgebra[ID] {

  val DANGLING: ID

  final val _reserved = Seq(DANGLING)

  final def retryUntilNotReserved(fn: => ID): ID = {

    for (i <- 1 to 10) {
      val result = fn
      if (!_reserved.contains(result)) return result
    }
    sys.error("cannot find non-reserved ID")
  }

  protected def _create(): ID
  def create(): ID = retryUntilNotReserved {
    _create()
  }

  def createRotator(): Rotator[ID]
}

object IDAlgebra {

  abstract class Rotator[ID] extends (ID => ID)

  case object ForUUID extends IDAlgebra[UUID] {

    override val DANGLING: UUID = UUID.fromString("dangling")

    override def _create(): UUID = UUID.randomUUID()

    override def createRotator() = RotatorImpl()

    case class RotatorImpl(seed: UUID = _create()) extends Rotator[UUID] {

      //TODO: need local caching?

      override def apply(v1: UUID): UUID = {
        val higher = v1.getMostSignificantBits ^ seed.getMostSignificantBits
        val lower = v1.getLeastSignificantBits ^ seed.getLeastSignificantBits
        new UUID(higher, lower)
      }
    }
  }
}
