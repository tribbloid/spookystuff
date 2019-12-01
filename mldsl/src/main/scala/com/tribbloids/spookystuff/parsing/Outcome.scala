package com.tribbloids.spookystuff.parsing

trait Outcome[+R] {

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

object Outcome {

  case class Builder(tokens: Seq[Pattern.Token], prevPhase: Phase) {

    //TODO: remove rendered useless by !!.--
//    object Skip extends Outcome[Nothing] {
//
//      override lazy val export: Option[Nothing] = None
//
//      override lazy val nextPhaseVec: PhaseVec = prevPhase._2
//    }

    /**
      * export [[com.tribbloids.spookystuff.parsing.Pattern.Token]]s as is and do not change [[PhaseVec]]
      */
    object `!!` extends Outcome[String] {

      override lazy val export: Option[String] = Some(Pattern.tokens2Str(tokens))

      override lazy val nextPhaseVec: PhaseVec = prevPhase._2
    }
  }

  case class AndThen[R, +R2](
      base: Outcome[R],
      exportFn: R => Option[R2] = { v: R =>
        None
      },
      phaseVecFn: PhaseVec => PhaseVec = { v: PhaseVec =>
        v
      }
  ) extends Outcome[R2] {

    override lazy val export: Option[R2] = base.export.flatMap(exportFn)

    override lazy val nextPhaseVec: PhaseVec = {
      phaseVecFn(base.nextPhaseVec)
    }
  }
}
