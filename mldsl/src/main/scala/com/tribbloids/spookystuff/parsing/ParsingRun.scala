package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FState.SubRules
import com.tribbloids.spookystuff.parsing.Rule.{CharToken, EndOfStream, Token}
import com.tribbloids.spookystuff.utils.BacktrackingIterator

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object ParsingRun {}

// only applies to interpolation parser, there should be many others
/**
  * heavily stateful bookkeeping object that tracks progress of parsing process
  * WIHTOUT object creation overhead! 1 object for 1 complete parsing run!
  * Resembles Fastparse 2.1.0 object with the same name
  */
case class ParsingRun(
    stream: Seq[Char],
    initialFState: FState,
    maxBacktracking: Int = 5
) {

  val input: Seq[Token] = {

    stream.map { cc =>
      CharToken(cc)
    } ++ Seq(EndOfStream)
  }

  val backtrackingMgr = BacktrackingManager(input, initialFState -> FStateMeta())

  def run() = {
    backtrackingMgr.untilTheEnd()

    backtrackingMgr.stack.toSeq.map { ls =>
      val captured = ls.spanString :+ ls.token
      val transition = ls.current
      captured -> transition
    }
  }
}
