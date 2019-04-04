package com.tribbloids.spookystuff.parsing

import java.util.UUID

import com.tribbloids.spookystuff.graph.{Algebra, DataAlgebra, Domain, FlowDSL, IDAlgebra, LocalGraph}

// decoupling, decoupling ...
trait GraphParser extends Domain {

  override type ID = UUID
  override type NodeData = Option[String]
  override type EdgeData = Option[String]
}

object GraphParser extends Algebra[GraphParser] {

  override def idAlgebra = IDAlgebra.UUIDAlgebra

  object DataAlgebraProto extends DataAlgebra[String] {

    override def plus(v1: String, v2: String): String = v1 + v2
  }

  override def nodeAlgebra = DataAlgebra.NoAmbiguity().Monadic
  override def edgeAlgebra = DataAlgebraProto.Monadic

  object SimpleDSL extends FlowDSL[GraphParser] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[GraphParser] = LocalGraph.BuilderImpl()

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
