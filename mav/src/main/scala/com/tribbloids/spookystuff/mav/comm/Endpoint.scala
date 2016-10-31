package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.actions.CaseInstanceRef

/**
  * Created by peng on 10/09/16.
  */
case class Endpoint(
                     //remember, one drone can have several telemetry endpoints: 1 primary and several backups (e.g. text message-based)
                     //TODO: implement backup mechanism
                     uris: Seq[String],
                     vehicleType: Option[String] = None
                   ) extends CaseInstanceRef