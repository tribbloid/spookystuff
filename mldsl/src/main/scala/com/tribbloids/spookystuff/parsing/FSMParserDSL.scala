package com.tribbloids.spookystuff.parsing

import FSMParserGraph.Layout._

import scala.language.implicitConversions

case class FSMParserDSL(core: Core) extends DSLLike {

  override type Self = FSMParserDSL

  override implicit def create(core: Core): FSMParserDSL = {

    FSMParserDSL(core)
  }
}

object FSMParserDSL extends FSMParserDSL(Core.empty)
