package com.tribbloids.spookystuff.parsing

trait Outcome[+R] {

  def export: Option[R]

  /**
    * mutate PhaseVec
    * @return has 4 outcomes:
    *         * Some -> jump to next state, with ParsingRun updated
    *         * None -> NoOp, no change to ParsingRun, keep searching using upcoming rules or even rules yields from upcoming tokens
    *         * throw BacktrackingException -> backtrack, captured by previous step processing and becomes an NoOp
    *         * throw other Exceptions -> fail immediately, no backtracking
    */
  def nextPhaseVecOpt: PhaseVec.Like

}

object Outcome {

  case class Factories(tokens: Seq[Pattern.Token], prevPhase: Phase) {

    object Skip extends Outcome[Nothing] {

      override lazy val export: Option[Nothing] = None

      override lazy val nextPhaseVecOpt: PhaseVec.Like = prevPhase._2
    }

    object `!!` extends Outcome[String] {

      override lazy val export: Option[String] = Some(Pattern.tokens2Str(tokens))

      override lazy val nextPhaseVecOpt: PhaseVec.Like = prevPhase._2
    }
  }

  case class AndThen[R, +R2](
      base: Outcome[R],
      exportFn: R => Option[R2] = { v: R =>
        None
      },
      phaseVecFn: PhaseVec => PhaseVec.Like = { v =>
        v
      }
  ) extends Outcome[R2] {

    override lazy val export: Option[R2] = base.export.flatMap(exportFn)

    override lazy val nextPhaseVecOpt: PhaseVec.Like = {
      base.nextPhaseVecOpt match {
        case v: PhaseVec => phaseVecFn(v)
        case v @ _       => v
      }

    }
  }
}
