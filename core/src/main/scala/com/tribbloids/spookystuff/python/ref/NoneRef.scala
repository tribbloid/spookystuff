package com.tribbloids.spookystuff.python.ref

trait NoneRef extends PyRef {
  override final val referenceOpt = Some("None")
  override final val delOpt = None
}
