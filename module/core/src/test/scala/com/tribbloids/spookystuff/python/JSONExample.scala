package com.tribbloids.spookystuff.python

import com.tribbloids.spookystuff.python.ref.JSONInstanceRef

/**
  * Created by peng on 27/11/16.
  */
case class JSONExample(
    a: Int,
    var bOpt: Option[String]
) extends JSONInstanceRef
