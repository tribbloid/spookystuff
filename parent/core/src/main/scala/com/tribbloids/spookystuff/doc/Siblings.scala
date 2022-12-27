package com.tribbloids.spookystuff.doc

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

//  def newBuilder[T <: Unstructured]: mutable.Builder[T, Siblings[T]] = new mutable.Builder[T, Siblings[T]] {
//
//    val buffer = new mutable.ArrayBuffer[T]()
//
//    override def +=(elem: T): this.type = {
//      buffer += elem
//      this
//    }
//
//    override def result(): Siblings[T] = new Siblings(buffer.toList)
//
//    override def clear(): Unit = buffer.clear()
//  }
}

class Siblings[+T <: Unstructured](
    override val delegate: List[T],
    val delimiter: String = " ",
    val formattedDelimiter: String = "\n"
) extends Elements[T](delegate) {

  override def text =
    if (texts.isEmpty) None
    else Some(texts.filter(_.nonEmpty).mkString(delimiter))

  override def code =
    if (codes.isEmpty) None
    else Some(codes.filter(_.nonEmpty).mkString(delimiter))

  override def formattedCode =
    if (formattedCodes.isEmpty) None
    else Some(formattedCodes.filter(_.nonEmpty).mkString(formattedDelimiter))

  override def ownText =
    if (ownTexts.isEmpty) None
    else Some(ownTexts.filter(_.nonEmpty).mkString(delimiter))

  override def boilerPipe =
    if (boilerPipes.isEmpty) None
    else Some(boilerPipes.filter(_.nonEmpty).mkString(delimiter))
}
