package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.GraphComponent.{Heads, Tails}
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator

trait BidirectionalDSL[T <: GraphSystem, G <: StaticGraph[T]] extends DSL[T, G] {

  object LL extends Facet
  object RR extends Facet

  case class DSLView(
      self: _GraphComponent,
      //tails or heads that doesn't belong to edges in self is tolerable
      tails: Map[Facet, _Tails] = Map.empty.withDefaultValue(Tails()),
      heads: _Heads = Heads()
  ) {

    lazy val _self = impl.fromComponent(self)

    def replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()): DSLView = {
      this.copy(
        self.replicate(m),
        tails.mapValues(_.replicate(m)),
        heads.replicate(m)
      )
    }

    def canConnectFrom(v: Facet): Boolean = {
      tails(v).seq.nonEmpty
    }

    case class Ops(
        topFacet: Facet,
        baseFacet: Facet
    ) {

      def base: DSLView = DSLView.this

//      def checkConnectivity(top: DSLView, topTails: _Tails): DSLView = {}

      def _mergeImpl(top: DSLView, topTails: _Tails): DSLView = {

        val result: G = impl.merge(
          base._self -> base.heads,
          top._self -> topTails
        )

        val newTails = Map(
          topFacet -> base.tails(topFacet),
          baseFacet -> top.tails(baseFacet)
        )
        val newHeads = top.heads

        DSLView(
          result,
          newTails,
          newHeads
        )
      }

      def merge(top: DSLView): DSLView = {

        _mergeImpl(top, top.tails(topFacet))
      }

      def rebase(top: DSLView): DSLView = {

        val topTails = top.tails(baseFacet).seq

        topTails.foldLeft(DSLView.this) { (self, edge) =>
          val tail = Tails(Seq(edge))
          self.Ops(topFacet, baseFacet)._mergeImpl(top.replicate(Mutator.noop[T]), tail)
        }
      }
    }

    object `>>` extends Ops(LL, RR)
    object `<<` extends Ops(RR, LL)
  }
}
