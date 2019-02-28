package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator

trait GraphSystem {

  type ID
  type NodeData
  type EdgeData
}

object GraphSystem {

  trait TypeSugers[T <: GraphSystem] {

    type ID = T#ID
    type NodeData = T#NodeData
    type EdgeData = T#EdgeData

    type _Mutator = Mutator[T]

    type _GraphComponent = GraphComponent[T]
    type _Heads = GraphComponent.Heads[T]
    type _Tails = GraphComponent.Tails[T]

    type _Element = Element[T]
    type _NodeLike = Element.NodeLike[T]
    type _LinkedNode = Element.LinkedNode[T]
    type _Edge = Element.Edge[T]
  }

  trait Sugars[T <: GraphSystem] extends TypeSugers[T] {

    implicit val systemBuilder: GraphSystem.Builder[T]

    def idAlgebra: IDAlgebra[ID] = systemBuilder.idAlgebra
    def nodeAlgebra: DataAlgebra[NodeData] = systemBuilder.nodeAlgebra
    def edgeAlgebra: DataAlgebra[EdgeData] = systemBuilder.edgeAlgebra
  }

  trait Builder[T <: GraphSystem] extends Sugars[T] {

    override implicit val systemBuilder = this

    override def idAlgebra: IDAlgebra[ID]
    override def nodeAlgebra: DataAlgebra[NodeData]
    override def edgeAlgebra: DataAlgebra[EdgeData]

    trait HasBuilder extends _GraphComponent {
      override implicit val systemBuilder = Builder.this
    }

    //  case class Dangling(info: Option[NodeInfo] = None) extends NodeLike
    object Dangling extends _NodeLike with HasBuilder {

      def _replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()): this.type = this

      override def _id: ID = idAlgebra.DANGLING
    }

    case class Node(
        info: T#NodeData,
        _id: T#ID = Builder.this.idAlgebra.create()
    ) extends _NodeLike
        with HasBuilder {

      //  def replicate: this.type

      //  override def tailNodes(v: Tail): Seq[Node[T]] = Seq(this)
      //
      //  override def headEdges(v: Head): Edge[T] =

      override def _replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()) = {
        this
          .copy(
            m.nodeFn(this.info),
            idRotator(this._id)
          )
      }

//      def asHead(info: EdgeData = edgeAlgebra.eye) = Edge(info, this._id -> Dangling._id)
//      def asTail(info: EdgeData = edgeAlgebra.eye) = Edge(info, Dangling._id -> this._id)
    }
  }
}
