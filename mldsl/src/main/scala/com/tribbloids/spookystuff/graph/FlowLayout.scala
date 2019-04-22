package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Layout.Facet
import com.tribbloids.spookystuff.graph.Module.Heads

import scala.language.implicitConversions

trait FlowLayout[D <: Domain] extends Layout[D] {

  import FlowLayout._

  lazy val facets = List(FromLeft, FromRight)

  trait DSL extends super.DSL {

    private implicit def core2Operand(v: Core): Operand = create(v)

    trait OperandLike extends super.OperandLike {

      lazy val `:>>` = core.Ops(FromLeft, FromRight)
      lazy val `<<:` = core.Ops(FromRight, FromLeft)

      // select from

      def from(filter: EdgeFilter[D]): Operand = {
        val newFrom = Heads[D](filter(core._graph))
        core.copy(
          fromOverride = Some(newFrom)
        )
      }

      //TODO: these symbols are lame
      def >-(filter: EdgeFilter[D]): Operand = from(filter)

      def and(filter: EdgeFilter[D]) = {
        val newFrom = Heads[D](filter(core._graph))
        val mergedFrom = Heads[D](core.from.seq ++ newFrom.seq)
        core.copy(
          fromOverride = Some(mergedFrom)
        )
      }

      def <>-(filter: EdgeFilter[D]): Operand = and(filter)

      // undirected

      def union(another: OperandLike): Operand = {
        `:>>`.union(another.core)
      }

      def U(another: OperandLike) = union(another)

      def ||(another: OperandLike) = union(another)

      // left > right

      def merge_>(right: OperandLike): Operand = `:>>`.merge(right.core)

      //    def merge(right: Interface) = merge_>(right)
      def >>>(right: OperandLike) = merge_>(right)

      def >(right: OperandLike) = merge_>(right)

      //TODO: fast-forward handling: if right is reused for many times,
      // ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
      def rebase_>(right: OperandLike): Operand = `:>>`.rebase(right.core)

      def rebase(right: OperandLike) = rebase_>(right)

      def >=>(right: OperandLike) = rebase_>(right)

      //this is really kind of ambiguous
      def commit_>(right: OperandLike): Operand = `:>>`.commit(right.core)

      def commit(right: OperandLike) = commit_>(right)

      def >->(right: OperandLike) = commit_>(right)

      // left < right
      //TODO: follow :>> & <<: convention

      def merge_<(left: OperandLike): Operand = `<<:`.merge(left.core)

      def egrem(prev: OperandLike) = prev.merge_<(this)

      def <<<(prev: OperandLike) = prev.merge_<(this)

      def <(prev: OperandLike) = prev.merge_<(this)

      def rebase_<(left: OperandLike): Operand = `<<:`.rebase(left.core)

      def esaber(prev: OperandLike) = prev.rebase_<(this)

      def <=<(prev: OperandLike) = prev.rebase_<(this)

      def commit_<(left: OperandLike): Operand = `<<:`.commit(left.core)

      def timmoc(left: OperandLike) = left.commit_<(this)

      def <-<(left: OperandLike) = left.commit_<(this)
    }
  }
}

object FlowLayout {

  object FromLeft extends Facet(">>-", ">>- -->")
  object FromRight extends Facet("-<<", "<-- -<<")
}
