package com.tribbloids.spookystuff.actions

trait RewriteRule[T] extends Serializable {

  def rewrite(v: T): Seq[T]
}

object RewriteRule {

  case class Rules[T](rules: Seq[RewriteRule[T]]) {

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
