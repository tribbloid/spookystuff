package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Rule.Token

case class Rule(
    token: Token,
    range: Range = Rule.Range.next,
    /**
      * mutate meta
      * @return has 4 outcomes:
      *         * Some -> jump to next state, with ParsingRun updated
      *         * None -> NoOp, no change to ParsingRun, keep searching using upcoming rules or even rules yields from upcoming tokens
      *         * throw BacktrackingException -> backtrack, captured by previous step processing and becomes an NoOp
      *         * throw other Exceptions -> fail immediately, no backtracking
      */
    forward: Rule.MetaStateMap = (_, v) => Some(v)
) {

  //this assumes that OutputSink is immutable, and object creation overhead won't hit us hard
}

object Rule {

  type MetaStateMap = (
      Seq[Token],
      FStateMeta
  ) => Option[FStateMeta]

  trait Token extends Any

  case class CharToken(v: Char) extends AnyVal with Token // inlining is subjective
  case object EOS extends Token //end of stream
//  case object BeginningOfStream extends AnyVal with Token

  // TODO: how to enable chaining of functions to define resolving as a parser combinator ??
//  class P(
//      override val token: Token,
//      override val range: Range = 0 to 0
//  ) extends Rule {}

  object Range {

    val next = 0 to 0
    val maxLength = 0 until Int.MaxValue
  }
}
