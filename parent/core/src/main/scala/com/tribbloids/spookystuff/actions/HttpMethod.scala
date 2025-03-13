package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.utils.http.HttpUtils

import java.net.URI

//TODO: handle RedirectException for too many redirections.
//@SerialVersionUID(7344992460754628988L)
abstract class HttpMethod(
    uri: String
) extends Export
    with Action.Driverless
    with Timed
    with Wayback {

  @transient lazy val uriOption: Option[URI] = {
    val uriStr = uri.trim()
    if (uriStr.isEmpty) None
    else Some(HttpUtils.uri(uriStr))
  }
}
