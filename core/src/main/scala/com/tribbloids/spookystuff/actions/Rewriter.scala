package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}

trait Rewriter[T] {

  /**
    * invoked on driver
    * @param v
    * @param schema
    * @return
    */
  def rewriteGlobally(v: T, schema: DataRowSchema): T = v

  /**
    * invoked on executors, immediately after interpolation
    * *IMPORTANT!* may be called several times, before or after GenPartitioner.
    * @param v
    * @param schema
    * @return
    */
  def rewriteLocally(v: T, schema: DataRowSchema): Option[T] = Some(v)
}
