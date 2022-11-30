package com.tribbloids.spookystuff.graph.example

import java.util.UUID

import com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph._

trait SimpleFlowGraph extends Domain {

  override type ID = UUID
  override type NodeData = Option[String]
  override type EdgeData = Option[String]
}

object SimpleFlowGraph extends Algebra[SimpleFlowGraph] {

  override def idAlgebra: IDAlgebra[
    graph.example.SimpleFlowGraph.ID,
    graph.example.SimpleFlowGraph.NodeData,
    graph.example.SimpleFlowGraph.EdgeData
  ] = IDAlgebra.UUIDAlgebra

  object DataAlgebraProto extends DataAlgebra[String] {

    override def add(v1: String, v2: String): String = v1 + v2
  }

  override def nodeAlgebra: DataAlgebra[graph.example.SimpleFlowGraph.NodeData] = DataAlgebra.NoAmbiguity().ForSome
  override def edgeAlgebra: DataAlgebra[graph.example.SimpleFlowGraph.EdgeData] = DataAlgebraProto.ForSome

  object Layout extends FlowLayout[SimpleFlowGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[SimpleFlowGraph] = new LocalGraph.BuilderImpl()

    override lazy val defaultFormat: Visualisation.Format[SimpleFlowGraph] = Formats.ShowOption
  }

  object DSL extends Layout.DSL {

    case class Operand[+M <: _Module](core: Layout.Core[M]) extends OperandLike[M] {}

    override def create[M <: _Module](core: Layout.Core[M]): Operand[M] = Operand(core)
  }
}
