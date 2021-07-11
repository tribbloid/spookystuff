package com.tribbloids.spookystuff.graph

trait GraphAlgebra[T <: Domain] extends GraphAlgebra.TypeSugars[T] {

  implicit def algebra: GraphAlgebra[T] = this

  def idAlgebra: IDAlgebra[ID, NodeData, EdgeData]
  def nodeAlgebra: DataAlgebra[NodeData]
  def edgeAlgebra: DataAlgebra[EdgeData]

  trait _Sugars extends GraphAlgebra.Sugars[T] {
    override implicit val algebra: GraphAlgebra[T] = GraphAlgebra.this
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
      GraphAlgebra.this.idAlgebra.fromNodeData(info)
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

object GraphAlgebra {

  trait TypeSugars[D <: Domain] {

    final type ID = D#ID
    final type NodeData = D#NodeData
    final type EdgeData = D#EdgeData

    final type DataMutator = DataAlgebra.Mutator[D]
    final type IDRotator = IDAlgebra.Rotator[ID]

    final type _Module = Module[D]
    final type _Heads = Module.Heads[D]
    final type _Tails = Module.Tails[D]

    final type _Element = Element[D]

    final type _NodeLike = Element.NodeLike[D]
    final type _Node = Element.Node[D]
    final type _NodeTriplet = Element.NodeTriplet[D]

    final type _Edge = Element.Edge[D]

    final type _VizFormat = Visualisation.Format[D]
    final def _vizFormat: Visualisation.Format.type = Visualisation.Format
  }

  trait Sugars[D <: Domain] extends TypeSugars[D] {

    implicit def algebra: GraphAlgebra[D]

    def idAlgebra: IDAlgebra[ID, NodeData, EdgeData] = algebra.idAlgebra
    def nodeAlgebra: DataAlgebra[NodeData] = algebra.nodeAlgebra
    def edgeAlgebra: DataAlgebra[EdgeData] = algebra.edgeAlgebra
  }
}
