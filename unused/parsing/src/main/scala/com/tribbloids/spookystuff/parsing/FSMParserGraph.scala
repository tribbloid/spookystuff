package com.tribbloids.spookystuff.parsing

import java.util.UUID
import com.tribbloids.spookystuff.graph.*
import com.tribbloids.spookystuff.parsing
import org.scalameta.ascii.layout.prefs.LayoutPrefsImpl

/**
  * State-machine based parser combinator that takes a graph and compile into a deterministic decision process ... that
  * consumes a stream of tokens and mutates state/output
  */
trait FSMParserGraph extends Domain {

  override type ID = UUID
  override type NodeData = FState.Type
  override type EdgeData = Option[Rule]
}

object FSMParserGraph extends Algebra[FSMParserGraph] {

  override def idAlgebra: IDAlgebra.UUIDAlgebra.type = IDAlgebra.UUIDAlgebra

  override def nodeAlgebra: DataAlgebra.MaxBy[NodeData] =
    DataAlgebra.MaxBy(
      {
        case FState.Ordinary => 0
        case FState.ROOT     => 1
        case FState.FINISH   => 2
      },
      Some(FState.Ordinary)
    )
  override def edgeAlgebra: DataAlgebra[parsing.FSMParserGraph.EdgeData] = DataAlgebra.NoAmbiguity[Rule]().ForSome

  object Layout extends FlowLayout[FSMParserGraph] {

    override lazy val defaultGraphBuilder: LocalGraph.BuilderImpl[FSMParserGraph] = new LocalGraph.BuilderImpl()

    override lazy val defaultFormat: Visualisation.Format[FSMParserGraph] = Formats.ShowOption.copy(
      asciiLayout = LayoutPrefsImpl(
        unicode = true,
//        compactify = false,
//        elevateEdges = false,
        doubleVertices = true,
        explicitAsciiBends = true
      )
    )

  }
}
