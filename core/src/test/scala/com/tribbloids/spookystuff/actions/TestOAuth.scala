package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.session.{OAuthKeys, Session}
import org.scalatest.tags.Retryable

/**
  * Adding OAuth parameters should not affect results of other queries
  */
@Retryable
class TestOAuth extends TestWget {

  import com.tribbloids.spookystuff.dsl._

  override def wget(uri: String) = {
    val action: OAuthV2 = OAuthV2(Wget(uri))
    val session = new Session(spooky)
    val effective = action.rewrite(session)
    assert(effective.uri !== uri)
    action
  }

  override def beforeEach() = {
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
