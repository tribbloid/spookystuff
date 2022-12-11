package com.tribbloids.spookystuff.utils.io

import org.apache.spark.ml.dsl.utils.data.EAVSystem

object ResourceMetadata extends EAVSystem {

  case class _EAV(
      override val internal: collection.Map[String, Any]
  ) extends ThisEAV {

    type Bound = Any

    object uri extends Attr[String]()

    object name extends Attr[String]()

    object `type` extends Attr[String]()

    object `content-type` extends Attr[String]()

    object length extends Attr[Long]()

    object `status-code` extends Attr[Int]()

    object `isDir` extends Attr[Boolean]()
  }

}
