package com.tribbloids.spookystuff.parsing

trait Rule {

  def token: Char
  def range: Range = 0 to 0

  /**
    * has 4 outcomes:
    * Some(nextState) -> jump to next state, with both sink and node updated
    * None -> NoOp, keep searching using upcoming rules or even rules yields from upcoming tokens
    * throw BacktrackingException -> backtrack, captured by previous step processing and becomes an NoOp
    * throw other Exceptions -> fail immediately
    * @return next state
    */
  def step(
      before: Seq[Char],
      currentState: ParserState
  ): Option[ParserState.Sink]
}

object Rule {

  // TODO: how to enable chaining of functions to define resolving as a parser combinator ??
  case class SimpleRule(
      token: Char,
      override val range: Range = 0 to 0
  ) extends Rule {

    override def step(before: Seq[Char], after: Seq[Char], currentState: ParserState): Option[ParserState.Sink] = ???
  }
}
