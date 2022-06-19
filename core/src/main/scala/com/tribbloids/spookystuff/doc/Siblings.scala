package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 18/07/15.
  */
object Siblings {}

class Siblings[+T <: Unstructured](
    override val originalSeq: List[T],
    val delimiter: String = " ",
    val formattedDelimiter: String = "\n"
) extends Elements[T](originalSeq) {

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
