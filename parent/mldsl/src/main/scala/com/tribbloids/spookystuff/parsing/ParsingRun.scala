package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.{CharToken, EndOfStream, Token}

object ParsingRun {

  case class ResultSeq(
      self: Seq[(Seq[Token], RuleOutcome[Any], Rule)]
  ) {

    lazy val outputs: Seq[Any] = self.flatMap(v => v._2.export)

    lazy val outputToString: String = outputs.mkString("\n")

    lazy val ioMap: Seq[(String, Option[Any])] = self.map { v =>
      Pattern.tokens2Str(v._1) -> v._2.export
    }

    lazy val ioMapToString: String = ioMap
      .map {
        case (k, Some(v)) =>
          s"$k\t-> $v"
        case (k, None) =>
          k
      }
      .mkString("\n")

    override def toString: String = ioMapToString
  }
}

// only applies to interpolation parser, there should be many others
/**
  * heavily stateful book-keeping object that tracks progress of parsing process WIHTOUT object creation overhead! 1
  * object for 1 complete parsing run! Resembles Fastparse 2.1.0 object with the same name
  */
case class ParsingRun(
    stream: Seq[Char],
    initialFState: FState,
    maxBacktracking: Int = 5 // TODO: enable it!
) {

  val input: Seq[Token] = {

    stream.map { cc =>
      CharToken(cc)
    } ++ Seq(EndOfStream)
  }

  val backtrackingMgr: BacktrackingManager = BacktrackingManager(input, initialFState -> PhaseVec.Eye)

  lazy val run: ParsingRun.ResultSeq = {
    backtrackingMgr.run_!()

    val seq = backtrackingMgr.stack.map { ls =>
      val captured = ls.spanTokens
      val result = ls.currentOutcome
      (captured, result._2, result._1)
    }
    ParsingRun.ResultSeq(seq)
  }
}
