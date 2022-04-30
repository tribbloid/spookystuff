package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.row.Field

import scala.language.{existentials, implicitConversions}

trait Alias[T, +R] extends GenExtractor[T, R] {

  def field: Field
}

object Alias {

  case class Impl[T, +R](
      child: GenExtractor[T, R],
      field: Field
  ) extends Alias[T, R]
      with GenExtractor.Wrapper[T, R] {}
}
