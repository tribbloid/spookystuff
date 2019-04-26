package com.tribbloids.spookystuff.parsing

import FSMParserGraph.Layout._

import scala.language.implicitConversions

/**
  * design to cover the following 4 cases:
  * - start parsing: Start
  * - match next char immediately: Start >>> P("{")
  * - match the first char after arbitrary non-key tokens Start >>> P_*("$")
  * - state change represented by a function (FStateMeta => FStateMeta): P("{").change(meta => ...)
  * - strip
  * - escape: go to same state with skip +1 P_*("$").escape
  * -
  */
object FSMParserDSL extends DSL {

  trait Operand extends OperandLike {

    lazy val fromLeftNode: Operand = Node(FState.Ordinary)
    lazy val fromRightNode: Operand = Node(FState.Ordinary)

    def ~>(top: Operand): Operand = {

      this >>> top.fromLeftNode >>> top
    }

    def <~(base: Operand): Operand = {

      this <<< this.fromRightNode <<< base
    }

//    lazy val compile: Combined = {
//
//      // add
//    }
  }

  case class Raw(core: Core) extends Operand {}

  override def create(core: Core): Operand = Raw(core)

  object EndOfParsing extends Operand {

    override lazy val core: Core = {
      val edge = Core.fromEdgeData(Some(Rule(Rule.EOS, Rule.Range.maxLength)))
      Core.fromNodeData(FState.EndOfParsing)
    }
  }

  class PatternLike(
      rule: Rule,
      loops: Seq[Pattern]
  ) extends Operand {

    override lazy val core: Core = Core.fromEdgeData(Some(rule))

    def ^^[T](fn: => T): T = ???

    def map(fn: Rule.MetaStateMap) = Pattern(rule.copy(forward = fn))

    def addLoop(v: Pattern): PatternLike = new PatternLike(rule, loops)
  }

  case class Pattern(
      rule: Rule
  ) extends PatternLike(rule, Nil) {}

  def P(v: Char) = Pattern(Rule(Rule.CharToken(v), Rule.Range.next))
  def P_*(v: Char) = Pattern(Rule(Rule.CharToken(v), Rule.Range.maxLength))
}
