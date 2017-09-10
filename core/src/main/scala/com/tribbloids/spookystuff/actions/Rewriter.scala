package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.execution.ExecutionContext

trait Rewriter[T] {

  def rewrite(v: T, ec: ExecutionContext): T
}
