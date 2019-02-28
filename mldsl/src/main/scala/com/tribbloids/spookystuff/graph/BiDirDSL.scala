package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.GraphComponent.{Heads, Tails}
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator

trait BiDirDSL[T <: GraphSystem, G <: StaticGraph[T]] extends DSL[T, G] {

  object LL extends Facet
  object RR extends Facet

  case class DSLView(
      self: _GraphComponent,
      tails: Map[Facet, _Tails] = Map.empty.withDefaultValue(Tails()),
      heads: _Heads = Heads()
  ) {

    def replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()): DSLView = {
      this.copy(
        self.replicate(m),
        tails.mapValues(_.replicate(m)),
        heads.replicate(m)
      )
    }

    case class Ops(
        src: Facet,
        tgt: Facet
    ) {

      def base: DSLView = DSLView.this

      def _mergeImpl(top: DSLView, topTails: _Tails): DSLView = {

        val result: G = graphBuilder.merge(
          base.self -> base.heads,
          top.self -> topTails
        )

        val newTails = Map(
          src -> base.tails(src),
          tgt -> top.tails(tgt)
        )
        val newHeads = top.heads

        DSLView(
          result,
          newTails,
          newHeads
        )
      }

      def merge(top: DSLView): DSLView = {

        _mergeImpl(top, top.tails(src))
      }

      def rebase(top: DSLView): DSLView = {

        val topTails = top.tails(tgt).vs

        topTails.foldLeft(DSLView.this) { (self, edge) =>
          val tail = Tails(Seq(edge))
          self.Ops(src, tgt)._mergeImpl(top.replicate(Mutator.noop[T]), tail)
        }
      }
    }

    object `:>` extends Ops(LL, RR)
    object `<:` extends Ops(RR, LL)
  }
}
