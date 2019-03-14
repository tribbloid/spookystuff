package com.tribbloids.spookystuff.graph.example

import java.util.UUID

import com.tribbloids.spookystuff.graph._

trait Simple extends Domain {

  override type ID = UUID
  override type NodeData = Option[String]
  override type EdgeData = Option[String]
}

object Simple extends Algebra[Simple] {

  override def idAlgebra = IDAlgebra.UUIDAlgebra

  object DataAlgebraProto extends DataAlgebra[String] {

    override def plus(v1: String, v2: String): String = v1 + v2
  }

  override def nodeAlgebra = DataAlgebraProto.Monadic
  override def edgeAlgebra = DataAlgebraProto.Monadic

  object SimpleImpl extends Impl {

    override type DD = Simple
    override type GProto[T <: Domain] = LocalGraph[T]
  }

}
