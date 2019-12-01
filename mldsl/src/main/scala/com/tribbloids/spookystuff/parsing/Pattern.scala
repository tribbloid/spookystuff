package com.tribbloids.spookystuff.parsing

import java.util.UUID

import com.tribbloids.spookystuff.parsing.Pattern.Token
import com.tribbloids.spookystuff.utils.{IDMixin, RangeArg}

case class Pattern(
    token: Token,
    range: RangeArg = Pattern.RangeArgs.next
) {

  override def toString = s"'$token' $range"

  // ID is to make sure that rules created by P/P_* operator can be referenced in the resultSet
  case class Rule[+R](
      resultFn: OutcomeFn[R]
  ) extends IDMixin {

    override val _id: UUID = UUID.randomUUID()

    def outer: Pattern = Pattern.this

    def token: Token = outer.token
    def range: RangeArg = outer.range

    override def toString: String = outer.toString
  }

  object `!!` extends Rule(Outcome.Factories(_, _).`!!`)
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

    val next: RangeArg = 0L to 0L
    val maxLength: RangeArg = 0L to Long.MaxValue
  }
}
