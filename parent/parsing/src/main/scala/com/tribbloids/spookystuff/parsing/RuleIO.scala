package com.tribbloids.spookystuff.parsing

import scala.language.implicitConversions

case class RuleIO[R](
    input: RuleInput,
    outcome: RuleOutcome[R]
) {

  override lazy val toString: String = {
    s"""
       |$input
       |vvv
       |$outcome
       |""".stripMargin
  }
}

object RuleIO {

  implicit def toInput(v: RuleIO[?]): RuleInput = v.input

  implicit def toOutcome[R](v: RuleIO[R]): RuleOutcome[R] = v.outcome
}
