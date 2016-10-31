package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.actions.{CaseInstanceRef, StaticRef}

/**
  * Created by peng on 29/10/16.
  */
object Connection extends StaticRef

case class Connection(
                       endpoint: Endpoint,
                       proxy: Proxy
                     ) extends CaseInstanceRef