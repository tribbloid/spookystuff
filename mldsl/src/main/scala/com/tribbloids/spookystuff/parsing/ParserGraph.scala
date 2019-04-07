package com.tribbloids.spookystuff.parsing

import java.util.UUID

import com.tribbloids.spookystuff.graph.{Algebra, DataAlgebra, Domain, FlowDSL, IDAlgebra, LocalGraph}

/**
  * State-machine based parser combinator that takes a graph and compile into a deterministic decision process
  * ... that consumes a stream of tokens and mutates state/output
  */
trait ParserGraph extends Domain {

  override type ID = UUID
  override type NodeData = Unit
  override type EdgeData = Option[Rule]
}

object ParserGraph extends Algebra[ParserGraph] {

  override def idAlgebra = IDAlgebra.UUIDAlgebra

  override def nodeAlgebra = DataAlgebra.NoAmbiguity()
  override def edgeAlgebra = DataAlgebra.NoAmbiguity().Monadic

  object DSL extends FlowDSL[ParserGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[ParserGraph] = LocalGraph.BuilderImpl()

    override lazy val defaultFormat = Formats.ShowData
  }

  import DSL._

  case class Face(
      core: Core
  ) extends Interface {

    override type Self = Face

    override def copyImplicitly(core: DSL.Core): Face = this.copy(core)

//    def compile():
  }

  object Face extends Face(Core.empty)
}
