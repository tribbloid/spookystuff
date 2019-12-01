package com.tribbloids.spookystuff.parsing.exception

import com.tribbloids.spookystuff.utils.TreeException

case class BacktrackableFailure(
    override val simpleMsg: String = "",
    cause: Throwable = null
) extends TreeException
    with BacktrackableMixin
