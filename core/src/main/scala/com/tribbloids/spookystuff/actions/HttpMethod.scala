package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Col, FR}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.utils.http.HttpUtils

import java.net.URI

//TODO: handle RedirectException for too many redirections.
//@SerialVersionUID(7344992460754628988L)
abstract class HttpMethod(
    uri: Col[String]
) extends Export
    with Driverless
    with Timed
    with Wayback {

  @transient lazy val uriOption: Option[URI] = {
    val uriStr = uri.value.trim()
    if (uriStr.isEmpty) None
    else Some(HttpUtils.uri(uriStr))
  }

  def resolveURI(pageRow: FetchedRow, schema: SpookySchema): Option[Lit[FR, String]] = {
    val first = this.uri
      .resolve(schema)
      .lift(pageRow)
      .flatMap(SpookyUtils.asOption[Any])
    //TODO: no need to resolve array output?

    val uriStr: Option[String] = first.flatMap {
      case element: Unstructured => element.href
      case str: String           => Option(str)
      case obj: Any              => Option(obj.toString)
      case _                     => None
    }
    val uriLit = uriStr.map(Lit.erased[String])
    uriLit
  }
}
