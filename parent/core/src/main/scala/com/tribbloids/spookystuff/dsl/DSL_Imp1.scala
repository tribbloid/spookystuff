package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row.Field

import scala.language.implicitConversions

trait DSL_Imp1 {

  import GenExtractor._

  // --------------------------------------------------

  implicit def symbol2Field(symbol: Symbol): Field =
    Option(symbol).map(v => Field(v.name)).orNull

  implicit def symbol2Get1(symbol: Symbol): Get =
    Get(symbol.name)

  implicit def symbol2Get2(symbol: Symbol): ExtractorView[Any] =
    new ExtractorView[Any](Get(symbol.name))

  implicit def symbol2DocExView(symbol: Symbol): DocExView =
    GetDocExpr(symbol.name)

  implicit def symbol2GetItr(symbol: Symbol): IterableExView[Any] =
    IterableExView(Get(symbol.name).GetSeq)
}
