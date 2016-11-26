package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.session.python.CaseInstanceRef

/**
  * Unfortunately, you'll never know if it is unreachable until its Python binding is created
  * @param connStrs
  * @param vehicleType
  */
case class Endpoint(
                     // remember, one drone can have several telemetry
                     // endpoints: 1 primary and several backups (e.g. text message-based)
                     // TODO: implement telemetry backup mechanism
                     connStrs: Seq[String],
                     vehicleType: Option[String] = None
                   ) extends CaseInstanceRef {

  def connStr = connStrs.head
}