package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.TreeException

class ResourceLockError(
    override val simpleMsg: String = "",
    val cause: Throwable = null
) extends TreeException {

  override def getCause: Throwable = cause
}
