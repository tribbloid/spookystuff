package com.tribbloids.spookystuff.graph

trait Algebra[T <: Domain] extends Algebra.TypeSugars[T] {

  implicit def algebra: Algebra[T] = this

  def idAlgebra: IDAlgebra[ID, NodeData, EdgeData]
  def nodeAlgebra: DataAlgebra[NodeData]
  def edgeAlgebra: DataAlgebra[EdgeData]

  trait _Sugars extends Algebra.Sugars[T] {
    override implicit val algebra = Algebra.this
  }
  object _Sugars extends _Sugars

  //  case class Dangling(info: Option[NodeInfo] = None) extends NodeLike
  object DANGLING extends Element.Node[T](nodeAlgebra.eye, idAlgebra.DANGLING) with _Sugars {

    assert(isDangling)

    override def toString = s"$idStr"
  }

  def createNode(
      info: NodeData = nodeAlgebra.eye,
      id: Option[ID] = None
  ): _Node = {
    val _id = id.getOrElse {
      Algebra.this.idAlgebra.fromNodeData(info)
    }

    if (_id != idAlgebra.DANGLING)
      Element.Node[T](info, _id)
    else
      DANGLING
  }

  def createEdge(
      info: EdgeData = edgeAlgebra.eye,
      qualifier: Seq[Any] = Nil,
      from_to: Option[(ID, ID)] = None
  ): _Edge = {

    val _from_to = from_to.getOrElse {
      idAlgebra.DANGLING -> idAlgebra.DANGLING
    }

    Element.Edge[T](info, qualifier, _from_to)
  }
}

object Algebra {

  trait TypeSugars[D <: Domain] {

    final type ID = D#ID
    final type NodeData = D#NodeData
    final type EdgeData = D#EdgeData

    final type _Mutator = Mutator[D]
    final type _Rotator = IDAlgebra.Rotator[ID]

    final type _Module = Module[D]
    final type _Heads = Module.Heads[D]
    final type _Tails = Module.Tails[D]

    final type _Element = Element[D]

    final type _NodeLike = Element.NodeLike[D]
    final type _Node = Element.Node[D]
    final type _LinkedNode = Element.NodeTriplet[D]

    final type _Edge = Element.Edge[D]

    final type _ShowFormat = Visualisation.Format[D]
    final def _ShowFormat = Visualisation.Format

//    final type _ElementView = ElementView[D]
  }

  trait Sugars[D <: Domain] extends TypeSugars[D] {

    implicit def algebra: Algebra[D]

    def idAlgebra: IDAlgebra[ID, NodeData, EdgeData] = algebra.idAlgebra
    def nodeAlgebra: DataAlgebra[NodeData] = algebra.nodeAlgebra
    def edgeAlgebra: DataAlgebra[EdgeData] = algebra.edgeAlgebra
  }
}
