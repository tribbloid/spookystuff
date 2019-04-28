package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FSMParserGraph.Layout._

import scala.language.implicitConversions

/**
  * design to cover the following 4 cases:
  * - start parsing: Start
  * - match next char immediately: Start >>> P("{")
  * - match the first char after arbitrary non-key tokens Start >>> P_*("$")
  * - state change represented by a function (FStateMeta => FStateMeta): P("{").change(meta => ...)
  * - strip
  * - escape: go to same state with skip +1 P_*("$").escape
  */
object FSMParserDSL extends DSL {

  class Operand[+M <: _Module](val core: Core[M], val entryNodeOpt: Option[algebra._Node] = None)
      extends OperandLike[M] {

    lazy val entry: Operand[_Node] = {
      val entryNode: _Node = entryNodeOpt.getOrElse(algebra.createNode(FState.Ordinary))
      create(entryNode)
    }

    lazy val initial: Operand[_Node] = {

      val node: algebra._Node = algebra.createNode(FState.START, Some(entry.self._id))
      create(node)
    }

    def :~>(top: Operand[_]): Operand[GG] = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top.entry >>> top
      }

      val core = (base >>> topWithEntry).core
      new Operand(core, Some(base.entry.self))
    }

    def <~:(top: Operand[_]): Operand[GG] = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top <<< top.entry
      }

      val core = (topWithEntry <<< base).core
      new Operand(core, Some(base.entry.self))
    }

    def :&(op: Operand[_Module]) = Loop(op)
    def &:(op: Operand[_Module]) = Loop(op)

    case class Loop(op: Operand[_Module]) {

      def :~>(top: Operand[_Module]): Operand[GG] = {

        val base = Operand.this
        val _top = new Operand(top.core, Some(op.entry.self))

        base :~> _top
      }

      def <~:(top: Operand[_Module]): Operand[GG] = {

        val base = Operand.this
        val _top = new Operand(top.core, Some(op.entry.self))

        _top <~: base
      }
    }

    override lazy val output: Core[GG] = {

      val prunedTails = this.core.tails.mapValues { tails =>
        val seq = tails.seq.filter(_.data.nonEmpty)
        tails.copyImplicitly(seq)
      }

      val pruned = this.core.copy(tails = prunedTails)

      (initial >>> create(pruned) <<< initial).core
    }

//    lazy val compiled = FState(core.NodeView.)
  }

  override def create[M <: _Module](core: Core[M]): Operand[M] = new Operand(core)

  object EOS extends Operand(Core.Edge(Some(Rule(Rule.EndOfStream, Rule.RangeArgs.maxLength))))

  object FINISH extends Operand(Core.Node(FState.FINISH))

  class PatternLike(
      rule: Rule,
      loops: Seq[Pattern]
  ) extends Operand(Core.Edge(Some(rule))) {

    def ^^[T](fn: => T): T = ???

    def map(fn: Rule.MetaStateMap) = Pattern(rule.copy(forward = fn))

    def addLoop(v: Pattern): PatternLike = new PatternLike(rule, loops)
  }

  case class Pattern(
      rule: Rule
  ) extends PatternLike(rule, Nil) {}

  def P(v: Char) = Pattern(Rule(Rule.CharToken(v), Rule.RangeArgs.next))
  def P_*(v: Char) = Pattern(Rule(Rule.CharToken(v), Rule.RangeArgs.maxLength))
}
