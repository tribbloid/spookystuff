package com.tribbloids.spookystuff.graph

import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.graph.Module.{Heads, Tails}
import com.tribbloids.spookystuff.utils.Types

import scala.collection.mutable

trait Element[T <: Domain] extends Module[T] {

  def toHeads(info: EdgeData): Module.Heads[T]
  lazy val asHeads: Module.Heads[T] = toHeads(algebra.edgeAlgebra.eye)

  def toTails(info: EdgeData): Module.Tails[T]
  lazy val asTails: Module.Tails[T] = toTails(algebra.edgeAlgebra.eye)

  def idStr: String
  def dataStr: String
}

object Element {

  case class Edge[T <: Domain](
      data: T#EdgeData,
      // required or there is no way to delete
      // 1 of many edges that has identical data & from_to in a multigraph
      qualifier: Seq[Any],
      from_to: (T#ID, T#ID)
      //      tt: EdgeType = EdgeType.`->`,
  )(
      implicit
      val algebra: Algebra[T]
  ) extends Element[T] {

    def from: ID = from_to._1
    def to: ID = from_to._2

    def idStr: String = idAlgebra.ids2Str(from_to)
    def dataStr: String = "" + data

    override lazy val toString: String = s"${idAlgebra.ids2Str(from_to)}: $data"

    override protected def _replicate(m: DataMutator)(
        implicit
        idRotator: Rotator[ID],
        node_+ : Types.Compose[NodeData]
    ): Edge[T] = {
      val newIDs = idRotator(from) -> idRotator(to)
      if (newIDs == from_to) this
      else
        Edge[T](
          m.edgeFn(data),
          qualifier,
          idRotator(from) -> idRotator(to)
        )
    }

    lazy val canBeHead: Boolean = to == algebra.DANGLING.samenessDelegatedTo
    lazy val canBeTail: Boolean = from == algebra.DANGLING.samenessDelegatedTo
    lazy val isDetached: Boolean = canBeHead && canBeTail

    override def toHeads(info: EdgeData): Module.Heads[T] =
      if (canBeHead) Heads[T](Seq(this))
      else Heads[T](Nil)
    override def toTails(info: EdgeData): Module.Tails[T] =
      if (canBeTail) Tails[T](Seq(this))
      else Tails[T](Nil)
  }

  /////////////////////////////////////

  trait NodeLike[T <: Domain] extends Element[T] with EqualBy {

    def isDangling: Boolean = {
      samenessDelegatedTo == algebra.idAlgebra.DANGLING
    }

    def data: NodeData
    def samenessDelegatedTo: ID

    def idStr: String = idAlgebra.id2Str(samenessDelegatedTo)
    def dataStr: String = "" + data

    override def toString: String = s"$idStr: $data"

    def toLinked(graphOpt: Option[StaticGraph[T]]): _NodeTriplet = {
      this match {
        case v: _NodeTriplet => v
        case v: _Node =>
          graphOpt match {
            case Some(graph) =>
              val result = graph.getLinkedNodes(Seq(v.samenessDelegatedTo)).values.head
              assert(result.data == this.data)
              result
            case _ =>
              new _NodeTriplet(v)
          }
      }
    }
    lazy val asLinked: _NodeTriplet = toLinked(None)

    def toHeads(info: EdgeData): Module.Heads[T] =
      Module.Heads(Seq(Edge[T](info, Nil, this.samenessDelegatedTo -> algebra.DANGLING.samenessDelegatedTo)))

    def toTails(info: EdgeData): Module.Tails[T] =
      Module.Tails(Seq(Edge[T](info, Nil, algebra.DANGLING.samenessDelegatedTo -> this.samenessDelegatedTo)))
  }

  case class Node[T <: Domain](
      data: T#NodeData,
      samenessDelegatedTo: T#ID
  )(
      implicit
      val algebra: Algebra[T]
  ) extends NodeLike[T] {

    override def _replicate(m: DataMutator)(
        implicit
        idRotator: Rotator[ID],
        node_+ : Types.Compose[NodeData]
    ): _Module = {
      val newID = idRotator(this.samenessDelegatedTo)
      if (newID == this.samenessDelegatedTo)
        this
      else
        Node[T](
          m.nodeFn(this.data),
          newID
        )
    }
  }

  class NodeTriplet[D <: Domain](
      val node: Node[D],
      val inbound: mutable.LinkedHashSet[D#ID] = mutable.LinkedHashSet.empty[D#ID],
      val outbound: mutable.LinkedHashSet[D#ID] = mutable.LinkedHashSet.empty[D#ID]
  ) extends NodeLike[D] {

    override def algebra: Algebra[D] = node.algebra

    override protected def _replicate(m: DataMutator)(
        implicit
        idRotator: Rotator[ID],
        node_+ : Types.Compose[NodeData]
    ): _NodeTriplet = {
      new NodeTriplet[D](
        node.replicate(m),
        inbound.map(idRotator),
        outbound.map(idRotator)
      )
    }

    lazy val edgeIDs: mutable.LinkedHashSet[D#ID] = inbound ++ outbound

    override def data: D#NodeData = node.data

    override def samenessDelegatedTo: ID = node.samenessDelegatedTo

    def inboundIDPairs: Seq[(ID, ID)] = inbound.toSeq.map { v =>
      v -> node.samenessDelegatedTo
    }

    def outboundIDPairs: Seq[(ID, ID)] = outbound.toSeq.map { v =>
      node.samenessDelegatedTo -> v
    }
  }
}
