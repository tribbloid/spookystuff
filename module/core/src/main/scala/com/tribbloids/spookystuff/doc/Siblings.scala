package com.tribbloids.spookystuff.doc

import scala.language.implicitConversions

/**
  * Created by peng on 18/07/15.
  */
object Siblings {

  implicit def unbox[T <: Node](self: Siblings[T]): List[T] = self.nodeSeq
}

class Siblings[+T <: Node](
    override val nodeSeq: List[T],
    val delimiter: String = " ",
    val formattedDelimiter: String = "\n"
) extends ManyNodes[T] {

//  override def text: Option[String] =
//    if (texts.isEmpty) None
//    else Some(texts.filter(_.nonEmpty).mkString(delimiter))
//
//  override def code: Option[String] =
//    if (codes.isEmpty) None
//    else Some(codes.filter(_.nonEmpty).mkString(delimiter))
//
//  override def formattedCode: Option[String] =
//    if (formattedCodes.isEmpty) None
//    else Some(formattedCodes.filter(_.nonEmpty).mkString(formattedDelimiter))
//
//  override def ownText: Option[String] =
//    if (ownTexts.isEmpty) None
//    else Some(ownTexts.filter(_.nonEmpty).mkString(delimiter))
//
//  override def boilerPipe: Option[String] =
//    if (boilerPipes.isEmpty) None
//    else Some(boilerPipes.filter(_.nonEmpty).mkString(delimiter))
}
