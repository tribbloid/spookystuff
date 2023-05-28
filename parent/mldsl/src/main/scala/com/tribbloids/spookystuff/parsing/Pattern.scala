package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token
import com.tribbloids.spookystuff.utils.RangeArg

case class Pattern(
    token: Token,
    range: RangeArg = Pattern.RangeArgs.next
) {

  override def toString: String = s"'$token' $range"

  // ID is to make sure that rules created by P/P_* operator can be referenced in the resultSet
  trait Rule[+R] {

    def fn(ev: RuleInput): RuleOutcome[R]

//    lazy val name: String =

    def outer: Pattern = Pattern.this

    def token: Token = outer.token
    def range: RangeArg = outer.range

    override def toString: String = outer.toString

    def andThen[R1 >: R, R2](_fn: RuleIO[R1] => RuleOutcome[R2]): Rule[R2] = {
      AndThen(this, _fn)
    }
  }

  case object !! extends Rule[String] {
    override def fn(ev: RuleInput): RuleOutcome[String] = ev.!!
  }

  case class AndThen[R, R2](
      base: Rule[R],
      _fn: RuleIO[R] => RuleOutcome[R2]
  ) extends Rule[R2] {

    override def fn(ev: RuleInput): RuleOutcome[R2] = {
      val o1 = base.fn(ev)
      _fn(RuleIO(ev, o1))
    }
  }

}

object Pattern {

  trait Token extends Any

  case class CharToken(v: Char) extends AnyVal with Token {
    override def toString: String = v.toString
  } // inlining is subjective
  case object EndOfStream extends Token {
    override def toString: String = "[EOS]"
  }

  def tokens2Str(tokens: Seq[Pattern.Token]): String = {
    val chars = tokens.flatMap {
      case Pattern.CharToken(v) => Some(v)
      case _                    => None
    }
    new String(chars.toArray)
  }

  object RangeArgs {

    val next: RangeArg = RangeArg(0L, 0L)
    val maxLength: RangeArg = RangeArg(0L, Long.MaxValue)
  }
}
