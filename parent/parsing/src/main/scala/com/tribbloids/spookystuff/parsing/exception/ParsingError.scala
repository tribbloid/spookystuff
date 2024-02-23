package com.tribbloids.spookystuff.parsing.exception

import com.tribbloids.spookystuff.commons.TreeThrowable

case class ParsingError(
    override val simpleMsg: String = "",
    cause: Throwable = null
) extends Exception
    with TreeThrowable
