package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Layout.Facet
import com.tribbloids.spookystuff.graph.Module.Heads

import scala.language.implicitConversions

trait FlowLayout[D <: Domain] extends Layout[D] {

  import FlowLayout._

  lazy val facets = List(FromLeft, FromRight)

  trait DSL extends super.DSL {

    private implicit def core2Operand[M <: _Module](v: Core[M]): Operand[M] = create(v)

    trait OperandLike[+M <: _Module] extends super.OperandLike[M] {

      lazy val `>>` = core.Ops(FromLeft, FromRight)
      lazy val `<<` = core.Ops(FromRight, FromLeft)

      // select from

      def from(filter: EdgeFilter[D]) = {
        val newFrom: _Heads = Heads[D](filter(core._graph))
        core.copy(
          fromOverride = Some(newFrom)
        )
      }

      //TODO: these symbols are lame
      def >-[N >: M <: _Module](filter: EdgeFilter[D]) = from(filter)

      //TODO: should be part of EdgeFilter
      def and(filter: EdgeFilter[D]) = {
        val newFrom = Heads[D](filter(core._graph))
        val mergedFrom = Heads[D](core.from.seq ++ newFrom.seq)
        core.copy(
          fromOverride = Some(mergedFrom)
        )
      }

      def <>-(filter: EdgeFilter[D]) = and(filter)

      // undirected

      def union(another: OperandLike[_]): Operand[GG] = {
        `>>`.union(another.core)
      }

      def U(another: OperandLike[_]) = union(another)

      def ||(another: OperandLike[_]) = union(another)

      // left > right

      def merge_>(right: OperandLike[_]): Operand[GG] = `>>`.merge(right.core)

      //    def merge(right: Interface) = merge_>(right)
      def >>>(right: OperandLike[_]) = merge_>(right)

      def >(right: OperandLike[_]) = merge_>(right)

      //TODO: fast-forward handling: if right is reused for many times,
      // ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
      def rebase_>(right: OperandLike[_]): Operand[_Module] = `>>`.rebase(right.core)

      def rebase(right: OperandLike[_]) = rebase_>(right)

      def >=>(right: OperandLike[_]) = rebase_>(right)

      //this is really kind of ambiguous
      def commit_>(right: OperandLike[_]): Operand[_Module] = `>>`.commit(right.core)

      def commit(right: OperandLike[_]) = commit_>(right)

      def >->(right: OperandLike[_]) = commit_>(right)

      // left < right
      //TODO: follow :>> & <<: convention

      def merge_<(left: OperandLike[_]): Operand[GG] = `<<`.merge(left.core)

      def egrem(prev: OperandLike[_]) = prev.merge_<(this)

      def <<<(prev: OperandLike[_]) = prev.merge_<(this)

      def <(prev: OperandLike[_]) = prev.merge_<(this)

      def rebase_<(left: OperandLike[_]): Operand[_Module] = `<<`.rebase(left.core)

      def esaber(prev: OperandLike[_]) = prev.rebase_<(this)

      def <=<(prev: OperandLike[_]) = prev.rebase_<(this)

      def commit_<(left: OperandLike[_]): Operand[_Module] = `<<`.commit(left.core)

      def timmoc(left: OperandLike[_]) = left.commit_<(this)

      def <-<(left: OperandLike[_]) = left.commit_<(this)
    }
  }
}

object FlowLayout {

  object FromLeft extends Facet(">>-", ">>- -->")
  object FromRight extends Facet("-<<", "<-- -<<")
}
