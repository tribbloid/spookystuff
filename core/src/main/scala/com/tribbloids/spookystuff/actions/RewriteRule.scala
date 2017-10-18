package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.DataRowSchema

trait RewriteRule[T] extends Serializable{

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: DataRowSchema): T
}

trait MonadicRewriteRule[T] extends Serializable{

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: DataRowSchema): Option[T]
}
