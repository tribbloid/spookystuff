package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.utils.http.HttpUtils

case class OAuthV2(original: Wget) extends HttpMethod(original.uri) { // TODO: this should be an HTTP method

  def temporaryDelegate(agent: Agent): Wget = { // this is not normal form as the delegate Wget won't last long, should not be cached

    val keys = agent.spooky.conf.oAuthKeysFactory.function0()
    if (keys == null) {
      throw new QueryException("need to set SpookyConf.oAuthKeys first")
    }
    val effectiveWget: Wget = original.uriOption match {
      case Some(uri) =>
        val signed =
          HttpUtils.OauthV2(uri.toString, keys.consumerKey, keys.consumerSecret, keys.token, keys.tokenSecret)
        original.copy(uri = signed)
      case None =>
        original
    }
    effectiveWget
  }

  override def doExeNoName(agent: Agent): Seq[Observation] = {

    val temporary = this.temporaryDelegate(agent)

    temporary.doExeNoName(agent).map {
      case doc: Doc =>
        doc.updated(uid = doc.uid.copy(backtrace = this.trace, export = this)(doc.uid.name))
      case others =>
        others
    }
  }

  override protected def originalName: String = original.name

  override protected def originalWayback: Option[Long] = original.wayback
}
