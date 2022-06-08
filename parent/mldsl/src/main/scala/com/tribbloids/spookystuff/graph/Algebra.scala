package com.tribbloids.spookystuff.graph

trait Algebra[T <: Domain] extends Algebra.TypeAliases[T] {

  implicit def algebra: Algebra[T] = this

  def idAlgebra: IDAlgebra[ID, NodeData, EdgeData]
  def nodeAlgebra: DataAlgebra[NodeData]
  def edgeAlgebra: DataAlgebra[EdgeData]

  trait _Aliases extends Algebra.Aliases[T] {
    override implicit val algebra: Algebra[T] = Algebra.this
  }
  object _Aliases extends _Aliases

  //  case class Dangling(info: Option[NodeInfo] = None) extends NodeLike
  object DANGLING extends Element.Node[T](nodeAlgebra.eye, idAlgebra.DANGLING) with _Aliases {

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

  trait TypeAliases[D <: Domain] {

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

    final type _ShowFormat = Visualisation.Format[D]
    final def _ShowFormat: Visualisation.Format.type = Visualisation.Format

//    final type _ElementView = ElementView[D]
  }

  trait Aliases[D <: Domain] extends TypeAliases[D] {

    implicit def algebra: Algebra[D]

    def idAlgebra: IDAlgebra[ID, NodeData, EdgeData] = algebra.idAlgebra
    def nodeAlgebra: DataAlgebra[NodeData] = algebra.nodeAlgebra
    def edgeAlgebra: DataAlgebra[EdgeData] = algebra.edgeAlgebra
  }
}
