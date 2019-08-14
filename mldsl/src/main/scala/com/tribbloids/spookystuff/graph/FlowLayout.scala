package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Layout.Facet

import scala.language.implicitConversions

trait FlowLayout[D <: Domain] extends Layout[D] {

  import FlowLayout._

  lazy val facets = Set(FromLeft, FromRight)

  trait DSL extends super.DSL {

    private implicit def core2Operand[M <: _Module](v: Core[M]): Operand[M] = create(v)

    trait OperandLike[+M <: _Module] extends super.OperandLike[M] {

      lazy val `>>` = core.Ops(FromLeft, FromRight)
      lazy val `<<` = core.Ops(FromRight, FromLeft)

      // undirected

      def union(another: Operand[_]): Operand[GG] = {
        `>>`.union(another.core)
      }

      final def U(another: Operand[_]) = union(another)

      final def ||(another: Operand[_]) = union(another)

      // left > right

      def merge_>(right: OperandLike[_]): Operand[GG] = `>>`.merge(right.core)

      //    def merge(right: Interface) = merge_>(right)
      final def >>>(right: OperandLike[_]) = merge_>(right)

      final def >(right: OperandLike[_]) = merge_>(right)

      //TODO: fast-forward handling: if right is reused for many times,
      // ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
      def rebase_>(right: OperandLike[_]): Operand[_Module] = `>>`.rebase(right.core)

//      final def rebase(right: OperandLike[_]) = rebase_>(right)

      final def >=>(right: OperandLike[_]) = rebase_>(right)

      //this is really kind of ambiguous
      def commit_>(right: OperandLike[_]): Operand[_Module] = `>>`.commit(right.core)

//      final def commit(right: OperandLike[_]) = commit_>(right)

      final def >->(right: OperandLike[_]) = commit_>(right)

      // left < right
      //TODO: follow :>> & <<: convention

      def merge_<(left: OperandLike[_]): Operand[GG] = `<<`.merge(left.core)

//      final def egrem(prev: OperandLike[_]) = prev.merge_<(this)

      final def <<<(prev: OperandLike[_]) = prev.merge_<(this)

      final def <(prev: OperandLike[_]) = prev.merge_<(this)

      def rebase_<(left: OperandLike[_]): Operand[_Module] = `<<`.rebase(left.core)

//      final def esaber(prev: OperandLike[_]) = prev.rebase_<(this)

      final def <=<(prev: OperandLike[_]) = prev.rebase_<(this)

      def commit_<(left: OperandLike[_]): Operand[_Module] = `<<`.commit(left.core)

//      final def timmoc(left: OperandLike[_]) = left.commit_<(this)

      final def <-<(left: OperandLike[_]) = left.commit_<(this)
    }
  }
}

object FlowLayout {

  object FromLeft extends Facet(">>-", ">>- -->") {
    override def positioning: Int = -1
  }
  object FromRight extends Facet("-<<", "<-- -<<") {
    override def positioning: Int = 1
  }
}
