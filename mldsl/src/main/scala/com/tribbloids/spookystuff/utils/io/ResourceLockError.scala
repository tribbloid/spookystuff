package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.TreeThrowable

class ResourceLockError(
    override val simpleMsg: String = "",
    val cause: Throwable = null
) extends Exception
    with TreeThrowable {

  override def getCause: Throwable = cause
}
