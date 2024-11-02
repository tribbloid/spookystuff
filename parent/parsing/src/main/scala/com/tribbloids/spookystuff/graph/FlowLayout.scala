package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Layout.Facet

import scala.language.implicitConversions

trait FlowLayout[D <: Domain] extends Layout[D] {

  import FlowLayout._

  lazy val facets: Set[Facet] = Set(FromLeft, FromRight)

  trait _DSL extends super.DSL {

    implicit private def core2Operand[M <: _Module](v: Core[M]): Operand[M] = create(v)

    trait _OperandLike[+M <: _Module] extends super.OperandLike[M] {

      lazy val RightOps: Core[M]#Ops = core.Ops(FromLeft, FromRight)
      lazy val LeftOps: Core[M]#Ops = core.Ops(FromRight, FromLeft)

      // undirected

      def union(another: Operand[_]): Operand[GG] = {
        RightOps.union(another.core)
      }

      final def U(another: Operand[_]): Operand[GG] = union(another)

      final def ||(another: Operand[_]): Operand[GG] = union(another)

      // left > right

      def compose_>(right: _OperandLike[_]): Operand[GG] = RightOps.compose(right.core)

      final def :>>(right: _OperandLike[_]): Operand[GG] = compose_>(right)

//      final def >(right: OperandLike[_]) = merge_>(right)

      // TODO: fast-forward handling: if right is reused for many times,
      // ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
      def mapHead_>(right: _OperandLike[_]): Operand[_Module] = RightOps.mapHead(right.core)

      final def :=>>(right: _OperandLike[_]): Operand[_Module] = mapHead_>(right)

      // this is really kind of ambiguous
      def append_>(right: _OperandLike[_]): Operand[_Module] = RightOps.append(right.core)

      final def :->(right: _OperandLike[_]): Operand[_Module] = append_>(right)

      // left < right
      // TODO: follow :>> & <<: convention

      def compose_<(left: _OperandLike[_]): Operand[GG] = LeftOps.compose(left.core)

      final def <<:(left: _OperandLike[_]): Operand[GG] = compose_<(left)

//      final def <(prev: OperandLike[_]) = prev.merge_<(this)

      def mapHead_<(left: _OperandLike[_]): Operand[_Module] = LeftOps.mapHead(left.core)

      final def <<=:(left: _OperandLike[_]): Operand[_Module] = mapHead_<(left)

      def append_<(left: _OperandLike[_]): Operand[_Module] = LeftOps.append(left.core)

      final def <-:(left: _OperandLike[_]): Operand[_Module] = append_<(left)
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
