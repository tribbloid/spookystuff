package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Rule.Token

trait Rule {

  def token: Token
  def range: Range = 0 to 0

  /**
    * mutate sink
    * @return has 4 outcomes:
    *         * Some -> jump to next state, with ParsingRun updated
    *         * None -> NoOp, no change to ParsingRun, keep searching using upcoming rules or even rules yields from upcoming tokens
    *         * throw BacktrackingException -> backtrack, captured by previous step processing and becomes an NoOp
    *         * throw other Exceptions -> fail immediately, no backtracking
    */
  def forward(
      preceding: Seq[Token],
      token: Token,
      meta: FStateMeta
  ): Option[FStateMeta]

  //this assumes that OutputSink is immutable, and object creation overhead won't hit us hard
}

object Rule {

  trait Token extends Any

  case class CharToken(v: Char) extends AnyVal with Token // inlining is subjective
  case object EOS extends Token //end of stream
//  case object BeginningOfStream extends AnyVal with Token

  // TODO: how to enable chaining of functions to define resolving as a parser combinator ??
//  class P(
//      override val token: Token,
//      override val range: Range = 0 to 0
//  ) extends Rule {}
}
