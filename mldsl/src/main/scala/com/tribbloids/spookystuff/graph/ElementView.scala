package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.utils.IDMixin

import scala.language.implicitConversions

trait ElementView[D <: Domain] extends Algebra.Sugars[D] with IDMixin {

  val core: DSL[D]#Core
  override final def algebra: Algebra[D] = core.algebra

  def element: _Element
  def format: _ShowFormat = _ShowFormat[D]()

  def inbound: Seq[ElementView[D]]
  def outbound: Seq[ElementView[D]]

  override final def _id = element

  case class ForwardTreeNode(
      /**
        * VERY IMPORTANT for cycle elimination
        */
      visited: Set[_Element] = Set.empty
  ) extends ElementTreeNode[D] {

    override val view = ElementView.this

    override def dirSymbol = "v "

    override val _children: Seq[ElementView[D]] = ElementView.this.outbound

    override implicit def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] =
      v.ForwardTreeNode(visited + element)
  }

  case class BackwardTreeNode(
      /**
        * VERY IMPORTANT for cycle elimination
        */
      visited: Set[_Element] = Set.empty
  ) extends ElementTreeNode[D] {

    override val view = ElementView.this

    override def dirSymbol = "^ "

    override val _children: Seq[ElementView[D]] = ElementView.this.inbound

    override implicit def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] = v.BackwardTreeNode(visited + element)
  }
}
