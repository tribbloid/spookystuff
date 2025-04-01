package com.tribbloids.spookystuff.doc

trait DocSelector {
  // also a magnet from string or JSoup

  def toString: String

//  def asJSoupEvaluator: Evaluator
}

object DocSelector {

  implicit class CSS(override val toString: String) extends DocSelector

  // TODO: will be enabled after all parsers can yield XML tree

//  implicit class Css(query: String) extends ElementQuery {
//    override def asJSoupEvaluator: Evaluator = QueryParser.parse(query)
//  }
}
