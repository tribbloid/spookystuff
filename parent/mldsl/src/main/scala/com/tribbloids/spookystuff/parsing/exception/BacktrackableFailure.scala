package com.tribbloids.spookystuff.parsing.exception

import com.tribbloids.spookystuff.utils.TreeThrowable

case class BacktrackableFailure(
    override val simpleMsg: String = "",
    cause: Throwable = null
) extends TreeThrowable
    with BacktrackableMixin
