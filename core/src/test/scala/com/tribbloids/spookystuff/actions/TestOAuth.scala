package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.session.{OAuthKeys, Session}
import org.scalatest.tags.Retryable

/**
  * Adding OAuth parameters should not affect results of other queries
  */
@Retryable
class TestOAuth extends TestWget {

  override def wget(uri: String): Action = {
    val action: OAuthV2 = OAuthV2(Wget(uri))
    val session = new Session(spooky)
    val effective = action.rewrite(session)
    assert(effective.uri !== uri)
    action
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spooky.spookyConf.oAuthKeysFactory = () =>
      OAuthKeys(
        "consumerKey",
        "consumerSecret",
        "token",
        "tokenSecret"
    )
  }
}
