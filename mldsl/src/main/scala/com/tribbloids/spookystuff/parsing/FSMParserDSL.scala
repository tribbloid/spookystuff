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

  class Operand[+M <: _Module](
      val core: Core[M],
      val entryNode: algebra._Node = algebra.createNode(FState.Ordinary)
  ) extends OperandLike[M] {

    lazy val entry: Operand[_Node] = {
      create(entryNode)
    }

    lazy val root: Operand[_Node] = {

      val node: algebra._Node = algebra.createNode(FState.ROOT, Some(entry.self._id))
      create(node)
    }

    def :~>(top: Operand[_]): Operand[GG] = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top.entry >>> top
      }

      val core = (base >>> topWithEntry).core
      new Operand(core, base.entry.self)
    }

    def <~:(top: Operand[_]): Operand[GG] = {

      val base = this

      val topWithEntry = top match {
        case FINISH => FINISH
        case _      => top <<< top.entry
      }

      val core = (topWithEntry <<< base).core
      new Operand(core, base.entry.self)
    }

    override def union(another: Operand[_]): Operand[GG] = {
      //reuse this.entryNode

      val proto = super.union(another)
      val e1 = this.entryNode
      val e2 = another.entryNode

      val collapseEntryNodes = proto.core.replicate() {
        case e2._id => e1._id
        case v @ _  => v
      }

      new Operand(collapseEntryNodes, e1)
    }

    def :&(op: Operand[_Module]) = Loop(op)
    def &:(op: Operand[_Module]) = Loop(op)

    case class Loop(op: Operand[_Module]) {

      def :~>(top: Operand[_Module]): Operand[GG] = {

        val base = Operand.this
        val _top = new Operand(top.core, op.entry.self)

        base :~> _top
      }

      def <~:(top: Operand[_Module]): Operand[GG] = {

        val base = Operand.this
        val _top = new Operand(top.core, op.entry.self)

        _top <~: base
      }
    }

    override lazy val output: Core[GG] = {

      val prunedTails = this.core.tails.mapValues { tails =>
        val seq = tails.seq.filter(_.data.nonEmpty)
        tails.copyImplicitly(seq)
      }

      val pruned = this.core.copy(tails = prunedTails)

      (root >>> create(pruned) <<< root).core
    }

    lazy val compiled: FState = {

      val nodeView = output.Views.fromNode(root.self)
      FState(nodeView)
    }

    def parse(input: Seq[Char]): ParsingRun.ResultSeq = {
      val result = ParsingRun(input, compiled).run
      result
    }
  }

  override def create[M <: _Module](core: Core[M]): Operand[M] = new Operand(core)

  protected def rule2Edge(p: Rule): FSMParserGraph.Layout.Core[FSMParserGraph.Layout._Edge] =
    Core.Edge(Some(p))

  case class Parser[+T](rule: Pattern#Rule[T]) extends Operand[_Edge](rule2Edge(rule)) {

    def andThen[T2](
        fn: T => Option[T2],
        vecFn: PhaseVec => PhaseVec.Like = { v =>
          v
        }
    ): Parser[T2] = Parser(
      rule.copy { (v1, v2) =>
        val base = rule.resultFn(v1, v2)
        Outcome.AndThen(base, fn, vecFn)
      }
    )

    def ^^[T2](fn: T => Option[T2]): Parser[T2] = andThen(fn)
    def ^^^(fn: T => Unit): Parser[Nothing] = ^^ { v =>
      fn(v)
      None
    }

    def %(vecFn: PhaseVec => PhaseVec.Like): Parser[T] = andThen(v => Some(v), vecFn)
  }

  object Parser {

    implicit def toRule[T](v: Parser[T]): Pattern#Rule[T] = v.rule
  }

  case class P(v: Pattern) {

    object !! extends Parser[String](v.!!)

    lazy val !- : Parser[String] = `!!`.^^ { vv =>
      Some(vv.dropRight(1))
    }

    lazy val -! : Parser[Char] = `!!`.^^ { vv =>
      Some(vv.last)
    }

    lazy val -- : Parser[Nothing] = `!!`.^^ { v =>
      None
    }

    lazy val escape: Parser[Nothing] = --.% { _ =>
      PhaseVec.NoOp(Some(1))
    }
  }

  object P {

    implicit def toRuleOp(v: P) = v.!!
    implicit def toRule[T](v: P): Pattern#Rule[String] = v.!!.rule
  }

  def P(v: Char): P = P(Pattern(Pattern.CharToken(v), Pattern.RangeArgs.next))
  def P_*(v: Char): P = P(Pattern(Pattern.CharToken(v), Pattern.RangeArgs.maxLength))

  def EOS_/ : P = P(Pattern(Pattern.EndOfStream, Pattern.RangeArgs.next))
  def EOS_* : P = P(Pattern(Pattern.EndOfStream, Pattern.RangeArgs.maxLength))
  def EOS: P = EOS_*

  //
  case object FINISH extends Operand(Core.Node(FState.FINISH))
}
