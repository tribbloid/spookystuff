package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.row.SpookySchema

trait Delta {

  type Resolve <: Delta.Resolved
  def Resolve: SpookySchema => Resolve
}

object Delta {

  trait Resolved {
    def outer: Delta

    def before: SpookySchema

    def after: SpookySchema
  }

  case class Append(
      extractor: Extractor[Any]*
  ) extends Delta {

    case class Resolve(before: SpookySchema) extends Resolved {
      override def outer: Delta = Append.this
    }
  }

  case class Replace(
      extractor: Extractor[Any]*
  ) extends Delta {

    case class Resolve(before: SpookySchema) extends Resolved {
      override def outer: Delta = Replace.this
    }
  }

  case class Delete(
  ) extends Delta
}
