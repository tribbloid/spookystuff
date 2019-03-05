package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.utils.IDMixin

import scala.collection.mutable

trait Element[T <: GraphSystem] extends GraphComponent[T] {}

object Element {

  trait NodeLike[T <: GraphSystem] extends Element[T] with IDMixin {

    def info: Option[T#NodeData]
    def _id: ID
  }

  case class Edge[T <: GraphSystem](
      info: Option[T#EdgeData],
      ids: (T#ID, T#ID)
      //      tt: EdgeType = EdgeType.`->`,
  )(
      implicit val systemBuilder: GraphSystem.Builder[T]
  ) extends Element[T] {

    def from: ID = ids._1
    def to: ID = ids._2

    override protected def _replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()) = {
      this
        .copy[T](
          m.edgeFn(info),
          idRotator(from) -> idRotator(to)
        )(systemBuilder)
    }

    lazy val isHead = to == systemBuilder.Dangling._id
    lazy val isTail = from == systemBuilder.Dangling._id
    lazy val isPassthrough = isHead && isTail
  }

  case class LinkedNode[T <: GraphSystem](
      node: NodeLike[T],
      inbound: mutable.LinkedHashSet[T#ID] = mutable.LinkedHashSet.empty[T#ID],
      outbound: mutable.LinkedHashSet[T#ID] = mutable.LinkedHashSet.empty[T#ID]
  )(
      override implicit val systemBuilder: GraphSystem.Builder[T]
  ) extends NodeLike[T] {

    override protected def _replicate(m: _Mutator)(
        implicit idRotator: Rotator[ID] = idAlgebra.createRotator()
    ): _LinkedNode = {
      this.copy[T](
        node.replicate(m),
        inbound.map(idRotator),
        outbound.map(idRotator)
      )
    }

    override def info: Option[T#NodeData] = node.info

    override def _id: ID = node._id
  }
}
