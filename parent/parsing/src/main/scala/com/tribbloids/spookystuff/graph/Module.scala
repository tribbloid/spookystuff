package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.commons.Types

import scala.language.implicitConversions

trait Module[T <: Domain] extends Algebra.Aliases[T] {

  protected def _replicate(m: DataMutator)(
      implicit
      idRotator: Rotator[ID],
      node_+ : Types.Compose[NodeData]
  ): _Module

  def replicate(m: DataMutator = DataAlgebra.Mutator.identity)(
      implicit
      idRotator: Rotator[ID],
      node_+ : Types.Compose[NodeData] = nodeAlgebra.add
  ): this.type =
    _replicate(m).asInstanceOf[this.type]
}

object Module {

  trait Edges[T <: Domain, Self <: Edges[T, Self]] extends Algebra.TypeAliases[T] {

    {
      sanityCheck()
    }

    def seq: Seq[Edge[T]]

    def sanityCheck(): Unit = {}

    implicit def copyImplicitly(v: Seq[Edge[T]]): Self

    def replicate(m: DataMutator)(
        implicit
        idRotator: IDRotator
    ): Self = {
      seq.map(_.replicate(m))
    }

    def ++(another: Self): Self = this.seq ++ another.seq

    def convert(map: Map[_Edge, _Edge]): Self = {
      val result: Self = seq.map { edge =>
        map.getOrElse(edge, edge)
      }

      if (result.seq.isEmpty) this.asInstanceOf[Self]
      else result
    }
  }

  case class Heads[T <: Domain](seq: Seq[Edge[T]] = Nil) extends Edges[T, Heads[T]] {

    override def sanityCheck(): Unit = {
      seq.foreach { v =>
        require(v.canBeHead, s"$v cannot be a head")
      }
    }

    implicit override def copyImplicitly(v: Seq[Edge[T]]): Heads[T] = copy(v.filter(_.canBeHead))
  }

  case class Tails[T <: Domain](seq: Seq[Edge[T]] = Nil) extends Edges[T, Tails[T]] {

    override def sanityCheck(): Unit = {

      seq.foreach { v =>
        require(v.canBeTail, s"$v cannot be a tail")
      }
    }

    implicit override def copyImplicitly(v: Seq[Edge[T]]): Tails[T] = copy(v.filter(_.canBeTail))
  }
}
