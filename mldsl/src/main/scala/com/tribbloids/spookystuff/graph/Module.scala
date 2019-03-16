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

    def seq: Seq[Edge[T]]

    implicit def copyImplicitly(v: Seq[Edge[T]]): Self

    def replicate(m: _Mutator)(implicit idRotator: _Rotator): Self = {
      seq.map(_.replicate(m))
    }

    def ++(another: Self): Self = this.seq ++ another.seq

    def convert(map: Map[_Edge, _Edge]): Self = {
      seq.map { edge =>
        map.getOrElse(edge, edge)
      }
    }
  }

  case class Heads[T <: Domain](seq: Seq[Edge[T]] = Nil) extends Edges[T, Heads[T]] {

    seq.foreach(_.canBeHead)

    override implicit def copyImplicitly(v: Seq[Edge[T]]): Heads[T] = copy(v)
  }

  case class Tails[T <: Domain](seq: Seq[Edge[T]] = Nil) extends Edges[T, Tails[T]] {

    seq.foreach(_.canBeTail)

    override implicit def copyImplicitly(v: Seq[Edge[T]]): Tails[T] = copy(v)
  }
}
