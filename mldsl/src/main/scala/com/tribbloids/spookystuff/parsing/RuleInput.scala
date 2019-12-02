package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token

case class RuleInput(
    span: Seq[Token],
    lookForward: Seq[Token],
    prevPhase: Phase
) {

  lazy val spanStr: String = Pattern.tokens2Str(span)
  lazy val lookForwardStr: String = Pattern.tokens2Str(lookForward)

  override lazy val toString: String = {
    s"""
       |phase: $prevPhase
       |${(span ++ lookForward).map(_.toString).mkString}
       |${span.map(_ => " ").dropRight(1).mkString}^
       |""".stripMargin
  }

  /**
    * export [[com.tribbloids.spookystuff.parsing.Pattern.Token]]s as is and do not change [[PhaseVec]]
    */
  object !! extends RuleOutcome[String] {

    override lazy val export: Option[String] = Some(spanStr)

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
