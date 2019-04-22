package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.utils.TreeException

//triggers a backtracking, instead of failing outright
case class BacktrackingException(
    override val simpleMsg: String = "",
    cause: Throwable = null
) extends TreeException
