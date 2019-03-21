package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge

import scala.language.implicitConversions

trait Module[T <: Domain] extends Algebra.Sugars[T] {

  protected def _replicate(m: _Mutator)(implicit idRotator: _Rotator): _Module

  def replicate(m: _Mutator = Mutator.replicate)(implicit idRotator: _Rotator): this.type =
    _replicate(m).asInstanceOf[this.type]
}

object Module {

  trait Edges[T <: Domain, Self <: Edges[T, Self]] extends Algebra.TypeSugars[T] {

    {
      sanityCheck()
    }

    def seq: Seq[Edge[T]]

    def sanityCheck(): Unit = {}

    implicit def copyImplicitly(v: Seq[Edge[T]]): Self

    def replicate(m: _Mutator)(implicit idRotator: _Rotator): Self = {
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

    override implicit def copyImplicitly(v: Seq[Edge[T]]): Heads[T] = copy(v.filter(_.canBeHead))
  }

  case class Tails[T <: Domain](seq: Seq[Edge[T]] = Nil) extends Edges[T, Tails[T]] {

    override def sanityCheck(): Unit = {

      seq.foreach { v =>
        require(v.canBeTail, s"$v cannot be a tail")
      }
    }

    override implicit def copyImplicitly(v: Seq[Edge[T]]): Tails[T] = copy(v.filter(_.canBeTail))
  }
}
