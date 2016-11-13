package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.session.python.CaseInstanceRef

/**
  * Created by peng on 10/09/16.
  */
object Endpoint {
//  val candidate: caching.ConcurrentSet[Endpoint] = caching.ConcurrentSet()

  //  Delegated to Link.existinga
  //  val used: caching.ConcurrentSet[Endpoint] = caching.ConcurrentSet()

  // won't be retried
}

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