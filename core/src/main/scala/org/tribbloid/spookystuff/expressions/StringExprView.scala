package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.dsl

/**
* Created by peng on 12/1/14.
*/
//TODO: is it possible to use reflective on this?
class StringExprView(self: Expr[String]) {

  import dsl._

  def replaceAll(regex: String, replacement: String): Expr[String] =
    self.andMap(_.replaceAll(regex, replacement), s"replaceAll($regex,$replacement)")

  def trim: Expr[String] = self.andMap(_.trim, "trim")
}