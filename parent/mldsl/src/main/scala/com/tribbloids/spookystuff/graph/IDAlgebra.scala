package com.tribbloids.spookystuff.graph

import java.util.UUID

import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.utils.UUIDUtils

trait IDAlgebra[ID, -NodeData, -EdgeData] {

  def DANGLING: ID

  lazy val reserved = Set(DANGLING) // will be excluded from rotation

  final def retryUntilNotReserved(fn: => ID): ID = {

    for (i <- 1 to 10) {
      val result = fn
      if (!reserved.contains(result)) return result
    }
    sys.error("cannot find non-reserved ID")
  }

  protected def _random(): ID
  def fromNodeData(v: NodeData): ID = retryUntilNotReserved {
    _random()
  }
  def fromEdgeData(v: EdgeData): ID = retryUntilNotReserved {
    _random()
  }

  def rotatorFactory(): () => Rotator[ID]

  def id2Str(v: ID): String =
    if (v == DANGLING) "??"
    else "" + v

  def ids2Str(vs: (ID, ID)): String = {
    vs.productIterator
      .map { id =>
        id2Str(id.asInstanceOf[ID])
      }
      .mkString(" ~> ")
  }
}

object IDAlgebra {

  trait Rotator[ID] extends (ID => ID)

  object Rotator {

    implicit class FromFn[ID](fn: ID => ID) extends Rotator[ID] {
      override def apply(v1: ID): ID = fn(v1)
    }

    abstract class WithFixedPoint[ID](fixed: Set[ID]) extends Rotator[ID] {

      // TODO: need local caching?

      final override def apply(v: ID): ID = {
        if (fixed.contains(v)) v
        else _doApply(v)
      }

      protected def _doApply(v: ID): ID
    }
  }

  case object UUIDAlgebra extends IDAlgebra[UUID, Any, Any] {

    override lazy val DANGLING: UUID = UUID.nameUUIDFromBytes(Array.empty)

    override def _random(): UUID = UUID.randomUUID()

    override def rotatorFactory() = RotatorFactory()

    case class RotatorFactory() extends (() => RotatorImpl) {

      override def apply(): RotatorImpl = {
        RotatorImpl()
      }
    }

    case class RotatorImpl(seed: UUID = _random()) extends Rotator.WithFixedPoint[UUID](reserved) {

      override def _doApply(v1: UUID): UUID = UUIDUtils.xor(v1, seed)
    }
  }
}
