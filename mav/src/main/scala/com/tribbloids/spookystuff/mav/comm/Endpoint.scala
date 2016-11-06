package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.session.python.CaseInstanceRef

/**
  * Created by peng on 10/09/16.
  */
object Endpoint {
  val all: caching.ConcurrentSet[Endpoint] = caching.ConcurrentSet()

  val used: caching.ConcurrentSet[Endpoint] = caching.ConcurrentSet()

  val unreachable: caching.ConcurrentSet[Endpoint] = caching.ConcurrentSet()


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

  val name: String = vehicleType.getOrElse("DRONE")// + "@" + connStrs.head

}