package com.tribbloids.spookystuff.graph.example

import java.util.UUID

import com.tribbloids.spookystuff.graph._

trait SimpleGraph extends Domain {

  override type ID = UUID
  override type NodeData = Option[String]
  override type EdgeData = Option[String]
}

object SimpleGraph extends Algebra[SimpleGraph] {

  override def idAlgebra = IDAlgebra.UUIDAlgebra

  object DataAlgebraProto extends DataAlgebra[String] {

    override def plus(v1: String, v2: String): String = v1 + v2
  }

  override def nodeAlgebra = DataAlgebra.NoAmbiguity().Monadic
  override def edgeAlgebra = DataAlgebraProto.Monadic

  object Layout extends FlowLayout[SimpleGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[SimpleGraph] = LocalGraph.BuilderImpl()

    override lazy val defaultFormat = Formats.ShowOption
  }

  object DSL extends Layout.DSL {

    case class Operand(core: Layout.Core) extends OperandLike {}

    override def create(core: Layout.Core): Operand = Operand(core)
  }
}
