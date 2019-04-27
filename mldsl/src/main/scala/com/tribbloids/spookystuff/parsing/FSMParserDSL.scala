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

  class Operand(val core: Core, val entryNodeOpt: Option[algebra._Node] = None) extends OperandLike {

    lazy val entryNode: algebra._Node = entryNodeOpt.getOrElse(algebra.createNode(FState.Ordinary))

    lazy val entry: Operand = create(entryNode)

    lazy val initialFState: Operand = {

      val node: algebra._Node = algebra.createNode(FState.START, Some(entryNode._id))
      create(node)
    }

    def :~>(top: Operand): Operand = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top.entry >>> top
      }

      val core = (base >>> topWithEntry).core
      new Operand(core, Some(base.entryNode))
    }

    def <~:(top: Operand): Operand = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top <<< top.entry
      }

      val core = (topWithEntry <<< base).core
      new Operand(core, Some(base.entryNode))
    }

    def :&(op: Operand) = Loop(op)
    def &:(op: Operand) = Loop(op)

    case class Loop(op: Operand) {

      def :~>(top: Operand): Operand = {

        val base = Operand.this
        val _top = new Operand(top.core, Some(op.entryNode))

        base :~> _top
      }

      def <~:(top: Operand): Operand = {

        val base = Operand.this
        val _top = new Operand(top.core, Some(op.entryNode))

        _top <~: base
      }
    }

    override lazy val output: Core = {

      val prunedTails = this.core.tails.mapValues { tails =>
        val seq = tails.seq.filter(_.data.nonEmpty)
        tails.copyImplicitly(seq)
      }

      val pruned = this.core.copy(tails = prunedTails)

      (initialFState >>> create(pruned) <<< initialFState).core
    }
  }

  override def create(core: Core): Operand = new Operand(core)

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
