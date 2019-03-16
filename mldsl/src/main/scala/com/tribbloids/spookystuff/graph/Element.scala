package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Module.{Heads, Tails}
import com.tribbloids.spookystuff.utils.IDMixin

import scala.collection.mutable

trait Element[T <: Domain] extends Module[T] {

  def toHeads(info: EdgeData): Module.Heads[T]
  lazy val asHeads: Module.Heads[T] = toHeads(algebra.edgeAlgebra.eye)

  def toTails(info: EdgeData): Module.Tails[T]
  lazy val asTails: Module.Tails[T] = toTails(algebra.edgeAlgebra.eye)

  def idStr: String
}

object Element {

  case class Edge[T <: Domain](
      data: T#EdgeData,
      ids: (T#ID, T#ID)
      //      tt: EdgeType = EdgeType.`->`,
  )(
      implicit val algebra: Algebra[T]
  ) extends Element[T] {

    def from: ID = ids._1
    def to: ID = ids._2

    def idStr: String = idAlgebra.ids2Str(ids)

    override lazy val toString = s"${idAlgebra.ids2Str(ids)}: $data"

    override protected def _replicate(m: _Mutator)(implicit idRotator: _Rotator): Edge[T] = {
      val newIDs = idRotator(from) -> idRotator(to)
      if (newIDs == ids) this
      else
        Edge[T](
          m.edgeFn(data),
          idRotator(from) -> idRotator(to)
        )
    }

    lazy val canBeHead = to == algebra.DANGLING._id
    lazy val canBeTail = from == algebra.DANGLING._id
    lazy val isDetached = canBeHead && canBeTail

    override def toHeads(info: EdgeData): Module.Heads[T] =
      if (canBeHead) Heads[T](Seq(this))
      else Heads[T](Nil)
    override def toTails(info: EdgeData): Module.Tails[T] =
      if (canBeTail) Tails[T](Seq(this))
      else Tails[T](Nil)
  }

  /////////////////////////////////////

  trait NodeLike[T <: Domain] extends Element[T] with IDMixin {

    def isDangling: Boolean = {
      _id == algebra.idAlgebra.DANGLING
    }

    def data: NodeData
    def _id: ID

    def idStr: String = idAlgebra.id2Str(_id)

    override def toString = s"$idStr: $data"

    def toLinked(graphOpt: Option[StaticGraph[T]]): _LinkedNode = {
      this match {
        case v: _LinkedNode => v
        case v: _Node =>
          graphOpt match {
            case Some(graph) =>
              val result = graph.getLinkedNodes(Seq(v._id)).values.head
              assert(result.data == this.data)
              result
            case _ =>
              new _LinkedNode(v)
          }
      }
    }
    lazy val asLinked = toLinked(None)

    def toHeads(info: EdgeData): Module.Heads[T] =
      Module.Heads(Seq(Edge[T](info, this._id -> algebra.DANGLING._id)))

    def toTails(info: EdgeData): Module.Tails[T] =
      Module.Tails(Seq(Edge[T](info, algebra.DANGLING._id -> this._id)))
  }

  case class Node[T <: Domain](
      data: T#NodeData,
      _id: T#ID
  )(
      implicit val algebra: Algebra[T]
  ) extends NodeLike[T] {

    override def _replicate(m: _Mutator)(implicit idRotator: _Rotator) = {
      val newID = idRotator(this._id)
      if (newID == this._id)
        this
      else
        Node[T](
          m.nodeFn(this.data),
          newID
        )
    }
  }

  class LinkedNode[T <: Domain](
      val node: Node[T],
      val inbound: mutable.LinkedHashSet[T#ID] = mutable.LinkedHashSet.empty[T#ID],
      val outbound: mutable.LinkedHashSet[T#ID] = mutable.LinkedHashSet.empty[T#ID]
  )(
      override implicit val algebra: Algebra[T]
  ) extends NodeLike[T] {

    override protected def _replicate(m: _Mutator)(
        implicit idRotator: _Rotator
    ): _LinkedNode = {
      new LinkedNode[T](
        node.replicate(m),
        inbound.map(idRotator),
        outbound.map(idRotator)
      )
    }

    override def data: T#NodeData = node.data

    override def _id: ID = node._id

    def inboundIDPairs = inbound.toSeq.map { v =>
      v -> node._id
    }

    def outboundIDPairs = outbound.toSeq.map { v =>
      node._id -> v
    }
  }
}
