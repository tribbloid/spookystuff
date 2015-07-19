package org.tribbloid.spookystuff.pages

import scala.collection.{mutable, SeqLike}

/**
 * Created by peng on 18/07/15.
 */
object Siblings {

  //  implicit def canBuildFrom[T <: Unstructured]: CanBuildFrom[Elements[Unstructured], T, Elements[T]] =
  //    new CanBuildFrom[Elements[Unstructured], T, Elements[T]] {
  //
  //      override def apply(from: Elements[Unstructured]): mutable.Builder[T, Elements[T]] = newBuilder[T]
  //
  //      override def apply(): mutable.Builder[T, Elements[T]] = newBuilder[T]
  //    }

  def newBuilder[T <: Unstructured]: mutable.Builder[T, Siblings[T]] = new mutable.Builder[T, Siblings[T]] {

    val buffer = new mutable.ArrayBuffer[T]()

    override def +=(elem: T): this.type = {
      buffer += elem
      this
    }

    override def result(): Siblings[T] = new Siblings(buffer.toList)

    override def clear(): Unit = buffer.clear()
  }
}

class Siblings[+T <: Unstructured](override val self: List[T]) extends Elements[T](self)
with Seq[T]
with SeqLike[T, Siblings[T]] {

  override def text = if (texts.isEmpty) None
  else Some(texts.filter(_.nonEmpty).mkString(" "))

  override def code = if (codes.isEmpty) None
  else Some(codes.filter(_.nonEmpty).mkString(" "))

  override def ownText = if (ownTexts.isEmpty) None
  else Some(ownTexts.filter(_.nonEmpty).mkString(" "))

  override def boilerPipe = if (boilerPipes.isEmpty) None
  else Some(boilerPipes.filter(_.nonEmpty).mkString(" "))

  override protected[this] def newBuilder = Siblings.newBuilder[T]
}
