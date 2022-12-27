package com.tribbloids.spookystuff.python.ref

trait NoneRef extends PyRef {
  final override val referenceOpt: Some[String] = Some("None")
  final override val delOpt: None.type = None
}
