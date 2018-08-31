package com.tribbloids.spookystuff.python

import com.tribbloids.spookystuff.python.ref.CaseInstanceRef

case class CaseExample(
    a: Int,
    var bOpt: Option[String]
//                        child1: Option[CaseExample] = None,
//                        child2: Option[JSONExample] = None
) extends CaseInstanceRef
