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

  object SimpleDSL extends FlowDSL[SimpleGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[SimpleGraph] = LocalGraph.BuilderImpl()

    override lazy val defaultFormat = Formats.ShowData
  }

  import SimpleDSL._

  case class Face(
      core: Core
  ) extends Interface {

    override type Self = Face

    override def copyImplicitly(core: SimpleDSL.Core): Face = this.copy(core)
  }

  object Face extends Face(Core.empty)
}
