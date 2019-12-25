package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.TreeThrowable

class ResourceLockError(
    override val simpleMsg: String = "",
    val cause: Throwable = null
) extends TreeThrowable {

  override def getCause: Throwable = cause
}
