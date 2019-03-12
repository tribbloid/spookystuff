package com.tribbloids.spookystuff.graph

trait Algebra[T <: Domain] extends Algebra.Sugars[T] {

  override implicit val algebra = this

  override def idAlgebra: IDAlgebra[ID, NodeData]
  override def nodeAlgebra: DataAlgebra[NodeData]
  override def edgeAlgebra: DataAlgebra[EdgeData]

  trait _Sugars extends Algebra.Sugars[T] {
    override implicit val algebra = Algebra.this
  }
  object _Sugars extends _Sugars

  //  case class Dangling(info: Option[NodeInfo] = None) extends NodeLike
  object DANGLING extends Element.Node[T](nodeAlgebra.eye, idAlgebra.DANGLING) with _Sugars {}

  def createNode(
      info: NodeData = nodeAlgebra.eye,
      id: Option[ID] = None
  ): _Node = {
    val _id = id.getOrElse {
      Algebra.this.idAlgebra.init(info)
    }

    assert(_id != idAlgebra.DANGLING)
    Element.Node[T](info, _id)
  }

  def createEdge(
      info: EdgeData = edgeAlgebra.eye,
      ids: Option[(ID, ID)] = None
  ): _Edge = {

    val _ids = ids.getOrElse {
      idAlgebra.DANGLING -> idAlgebra.DANGLING
    }

    Element.Edge[T](info, _ids)
  }

  object PASSTHROUGH
      extends Element.Edge[T](
        edgeAlgebra.eye,
        DANGLING._id -> DANGLING._id
      )
}

object Algebra {

  trait TypeSugars[T <: Domain] {

    type ID = T#ID
    type NodeData = T#NodeData
    type EdgeData = T#EdgeData

    type _Mutator = Mutator[T]
    type _Rotator = IDAlgebra.Rotator[ID]

    type _Module = Module[T]
    type _Heads = Module.Heads[T]
    type _Tails = Module.Tails[T]

    type _Element = Element[T]

    type _NodeLike = Element.NodeLike[T]
    type _Node = Element.Node[T]
    type _LinkedNode = Element.LinkedNode[T]

    type _Edge = Element.Edge[T]

    type _ShowFormat = Visualisation.Format[T]
    def _ShowFormat = Visualisation.Format
  }

  trait Sugars[T <: Domain] extends TypeSugars[T] {

    implicit val algebra: Algebra[T]

    def idAlgebra: IDAlgebra[ID, NodeData] = algebra.idAlgebra
    def nodeAlgebra: DataAlgebra[NodeData] = algebra.nodeAlgebra
    def edgeAlgebra: DataAlgebra[EdgeData] = algebra.edgeAlgebra
  }
}
