package com.tribbloids.spookystuff.actions

trait NormaliseRule[T] extends Serializable {

  def rewrite(v: T): Seq[T]
}

object NormaliseRule {

  case class Rules[T](rules: Seq[NormaliseRule[T]]) {

    def rewriteAll(values: Seq[T]): Seq[T] = {
      val result = rules.foldLeft(values) { (opt, rewriter) =>
        opt.flatMap { trace =>
          rewriter.rewrite(trace)
        }
      }
      result
    }
  }
}
