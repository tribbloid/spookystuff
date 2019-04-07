package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Rules.SubRules

import scala.annotation.tailrec

// combine Rules & metadata
case class ParserState(
    rules: Rules,
    sink: ParserState.Sink
) {

  def step(input: Input): Option[Input] = {

    var ii = input.skip
    var cacheII = 0

    @tailrec
    def getEffectiveSubRules(ii: Int): SubRules = {

      val cached = rules.subRuleCache(cacheII)
      val range = cached._1
      if (range.contains(ii)) {
        cached._2
      } else {
        cacheII += 1
        getEffectiveSubRules(ii)
      }
    }

    for (ii <- input.skip until input.stream.length) {

      val char = input.stream(ii)
      val subRules = getEffectiveSubRules(ii)

      val rules: Seq[(Rule, Rules)] = subRules.mapView.getOrElse(char, Nil)
      // find the first that applies:

      lazy val step = rules.foreach { rule =>
        val nextOpt = rule._1.step(
          )
      }
    }

    ???
  }
}

object ParserState {

  // this only applies to interpolation parser, there should be many others
  case class Sink(matches: Seq[Range], depth: Int = 0)
}
