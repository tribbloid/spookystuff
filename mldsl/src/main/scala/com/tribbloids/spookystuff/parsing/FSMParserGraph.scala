package com.tribbloids.spookystuff.parsing

import java.util.UUID

import com.tribbloids.spookystuff.graph.{Algebra, DataAlgebra, Domain, FlowLayout, IDAlgebra, LocalGraph}

/**
  * State-machine based parser combinator that takes a graph and compile into a deterministic decision process
  * ... that consumes a stream of tokens and mutates state/output
  */
trait FSMParserGraph extends Domain {

  override type ID = UUID
  override type NodeData = FState.Type
  override type EdgeData = Option[Rule]
}

object FSMParserGraph extends Algebra[FSMParserGraph] {

  override def idAlgebra = IDAlgebra.UUIDAlgebra

  override def nodeAlgebra = DataAlgebra.NoAmbiguity()
  override def edgeAlgebra = DataAlgebra.NoAmbiguity().Monadic

  object Layout extends FlowLayout[FSMParserGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[FSMParserGraph] = LocalGraph.BuilderImpl()

    override lazy val defaultFormat = Formats.ShowOption
  }

  import Layout._

//  case class API(
//      core: Core
//  ) extends OperandLike {
//
//    override type Operand = API
//
//    override def create(core: DSL.Core): API = API(core)
//
////    def compile():
//  }
//
//  object API extends API(Core.empty)
}
