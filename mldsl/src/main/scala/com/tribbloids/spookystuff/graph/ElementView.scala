package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.utils.IDMixin

import scala.language.implicitConversions

trait ElementView[I <: Impl] extends Impl.Sugars[I] with IDMixin {

  val core: DSL[I]#Core

  def element: _Element
  def format: _ShowFormat = _ShowFormat[T]()

  def inbound: Seq[ElementView[I]]
  def outbound: Seq[ElementView[I]]

  override final def _id = element

  def edgeOnlyPrefix(v: String): String = element match {
    case vv: _Edge => v
    case _         => ""
  }

  case class ForwardTreeNode(
      /**
        * VERY IMPORTANT for cycle elimination
        */
      visited: Set[_Element] = Set.empty
  ) extends ElementTreeNode[I] {

    override val view = ElementView.this
    override val prefix: String = edgeOnlyPrefix(":>>>")
    override val _children: Seq[ElementView[I]] = ElementView.this.outbound

    override implicit def copyImplicitly(v: ElementView[I]): ElementTreeNode[I] = v.ForwardTreeNode(visited + element)
  }

  case class BackwardTreeNode(
      /**
        * VERY IMPORTANT for cycle elimination
        */
      visited: Set[_Element] = Set.empty
  ) extends ElementTreeNode[I] {

    override val view = ElementView.this
    override val prefix: String = edgeOnlyPrefix("<<<:")
    override val _children: Seq[ElementView[I]] = ElementView.this.inbound

    override implicit def copyImplicitly(v: ElementView[I]): ElementTreeNode[I] = v.BackwardTreeNode(visited + element)
  }
}
