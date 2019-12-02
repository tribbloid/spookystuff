package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token

case class RuleInput(
    span: Seq[Token],
    lookForward: Seq[Token],
    prevPhase: Phase
) {

  /**
    * export [[com.tribbloids.spookystuff.parsing.Pattern.Token]]s as is and do not change [[PhaseVec]]
    */
  object !! extends RuleOutcome[String] {

    override lazy val export: Option[String] = Some(Pattern.tokens2Str(span))

    override lazy val nextPhaseVec: PhaseVec = prevPhase._2
  }

  //TODO: remove rendered useless by !!.--
  //    object Skip extends Outcome[Nothing] {
  //
  //      override lazy val export: Option[Nothing] = None
  //
  //      override lazy val nextPhaseVec: PhaseVec = prevPhase._2
  //    }
}
