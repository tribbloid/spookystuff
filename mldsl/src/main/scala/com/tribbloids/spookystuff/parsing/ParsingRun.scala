package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.{CharToken, EndOfStream, Token}

import scala.collection.mutable.ArrayBuffer

object ParsingRun {

  case class ResultSeq(
      self: Seq[(ArrayBuffer[Token], Outcome[Any], Rule)]
  ) {

    lazy val exports: Seq[Any] = self.flatMap(v => v._2.export)

    lazy val strRepr: String = exports.mkString("\n")

    lazy val str2Exports: Seq[(String, Option[Any])] = self.map { v =>
      Pattern.tokens2Str(v._1) -> v._2.export
    }
  }
}

// only applies to interpolation parser, there should be many others
/**
  * heavily stateful bookkeeping object that tracks progress of parsing process
  * WIHTOUT object creation overhead! 1 object for 1 complete parsing run!
  * Resembles Fastparse 2.1.0 object with the same name
  */
case class ParsingRun(
    stream: Seq[Char],
    initialFState: FState,
    maxBacktracking: Int = 5 //TODO: enable it!
) {

  val input: Seq[Token] = {

    stream.map { cc =>
      CharToken(cc)
    } ++ Seq(EndOfStream)
  }

  val backtrackingMgr = BacktrackingManager(input, initialFState -> PhaseVec())

  lazy val run: ParsingRun.ResultSeq = {
    backtrackingMgr.run_!()

    val seq = backtrackingMgr.stack.reverse.map { ls =>
      val captured = ls.spanTokens :+ ls.token
      val result = ls.currentOutcome
      (captured, result._2, result._1)
    }
    ParsingRun.ResultSeq(seq)
  }
}
