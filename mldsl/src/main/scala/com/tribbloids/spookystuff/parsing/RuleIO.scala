package com.tribbloids.spookystuff.parsing

import scala.language.implicitConversions

case class RuleIO[R](
    input: RuleInput,
    outcome: RuleOutcome[R]
) {}

object RuleIO {

  implicit def toInput(v: RuleIO[_]): RuleInput = v.input

  implicit def toOutcome[R](v: RuleIO[R]): RuleOutcome[R] = v.outcome
}
