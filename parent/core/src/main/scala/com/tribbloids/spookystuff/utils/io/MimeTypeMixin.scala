package com.tribbloids.spookystuff.utils.io

trait MimeTypeMixin {
  self: Resource[_] =>

  final override def getType: String = {

    val contentType = getContentType

    if (Resource.mimeIsDir(contentType)) Resource.DIR
    else Resource.FILE
  }
}
