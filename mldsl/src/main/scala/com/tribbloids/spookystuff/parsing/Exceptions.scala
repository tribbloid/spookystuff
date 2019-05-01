package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.utils.TreeException

object Exceptions {

  case class BacktrackingFailure(
      override val simpleMsg: String = "",
      cause: Throwable = null
  ) extends TreeException

  case class ParsingError(
      override val simpleMsg: String = "",
      cause: Throwable = null
  ) extends TreeException

}
