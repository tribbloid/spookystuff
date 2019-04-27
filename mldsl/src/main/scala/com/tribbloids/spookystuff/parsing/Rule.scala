package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Rule.Token
import com.tribbloids.spookystuff.utils.RangeArg

case class Rule(
    token: Token,
    range: RangeArg = Rule.RangeArgs.next,
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

  override def toString = s"'$token' $range"

  //this assumes that OutputSink is immutable, and object creation overhead won't hit us hard
}

object Rule {

  type MetaStateMap = (
      Seq[Token],
      FStateMeta
  ) => Option[FStateMeta]

  trait Token extends Any

  case class CharToken(v: Char) extends AnyVal with Token {
    override def toString: String = v.toString
  } // inlining is subjective
  case object EndOfStream extends Token {
    override def toString: String = "[EOS]"
  } //end of stream
//  case object BeginningOfStream extends AnyVal with Token

  // TODO: how to enable chaining of functions to define resolving as a parser combinator ??
//  class P(
//      override val token: Token,
//      override val range: Range = 0 to 0
//  ) extends Rule {}

  object RangeArgs {

    val next: RangeArg = 0L to 0L
    val maxLength: RangeArg = 0L to Long.MaxValue
  }
}
