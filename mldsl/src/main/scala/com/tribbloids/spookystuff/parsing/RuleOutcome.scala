package com.tribbloids.spookystuff.parsing

trait RuleOutcome[+R] {

  def export: Option[R]

  /**
    * mutate PhaseVec
    * @return has 4 outcomes:
    *         Normal -> jump to next state, with ParsingRun updated
    *         [[com.tribbloids.spookystuff.parsing.PhaseVec.NoOp]] -> no change to ParsingRun, keep searching using upcoming rules or even rules yields from upcoming tokens
    *         throw [[com.tribbloids.spookystuff.parsing.exception.BacktrackableFailure]] -> backtrack, captured by previous step processing and becomes an NoOp
    *         throw other Exceptions -> fail immediately, no backtracking
    */
  def nextPhaseVec: PhaseVec
}

object RuleOutcome {

  case class O[R](
      override val export: Option[R],
      override val nextPhaseVec: PhaseVec
  ) extends RuleOutcome[R]

//  case class AndThen[R, +R2](
//      base: Outcome[R],
//      exportFn: R => Option[R2] = { v: R =>
//        None
//      },
//      phaseVecFn: PhaseVec => PhaseVec = { v: PhaseVec =>
//        v
//      }
//  ) extends Outcome[R2] {
//
//    override lazy val export: Option[R2] = base.export.flatMap(exportFn)
//
//    override lazy val nextPhaseVec: PhaseVec = {
//      phaseVecFn(base.nextPhaseVec)
//    }
//  }
}
