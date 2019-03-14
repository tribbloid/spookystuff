package com.tribbloids.spookystuff.graph.example

import com.tribbloids.spookystuff.graph.example.Simple.SimpleImpl
import com.tribbloids.spookystuff.graph.{FlowDSL, LocalGraph}

object SimpleDSL extends FlowDSL[SimpleImpl.type] {

  override lazy val impl: LocalGraph.BuilderImpl[Simple] = LocalGraph.BuilderImpl()

  override lazy val defaultFormat = Formats.ShowData
}
