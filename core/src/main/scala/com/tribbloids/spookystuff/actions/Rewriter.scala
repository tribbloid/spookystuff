package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.DataRowSchema

trait Rewriter[T] {

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: DataRowSchema): T = v
}

trait MonadicRewriter[T] {

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: DataRowSchema): Option[T] = Some(v)
}
