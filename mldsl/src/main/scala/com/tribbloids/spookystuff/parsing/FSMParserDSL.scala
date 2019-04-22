package com.tribbloids.spookystuff.parsing

import FSMParserGraph.Layout._

import scala.language.implicitConversions

object FSMParserDSL extends FSMParserGraph.Layout.DSL {

  case class Operand(core: Core) extends OperandLike {}

  override def create(core: Core): Operand = Operand(core)
}
