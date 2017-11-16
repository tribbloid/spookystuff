package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.SpookySchema

trait RewriteRule[T] extends Serializable{

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: SpookySchema): T
}

trait MonadicRewriteRule[T] extends Serializable{

  /**
    * @param v
    * @param schema
    * @return
    */
  def rewrite(v: T, schema: SpookySchema): Option[T]
}
