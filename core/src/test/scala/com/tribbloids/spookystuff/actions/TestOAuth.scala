package com.tribbloids.spookystuff.actions

import org.scalatest.tags.Retryable
import com.tribbloids.spookystuff.session.{NoDriverSession, OAuthKeys}

/**
 * Adding OAuth parameters should not affect results of other queries
 */
@Retryable
class TestOAuth extends TestWget {

  import com.tribbloids.spookystuff.dsl._

  override def wget(uri: String) = {
    val action: OAuthV2 = OAuthV2(Wget(uri))
    val session = new NoDriverSession(spooky)
    val effective = action.rewrite(session)
    assert(effective.uri !== uri)
    action
  }

  override def setUp() = {
    super.setUp()

    spooky.conf.oAuthKeysFactory = () => OAuthKeys(
      "consumerKey",
      "consumerSecret",
      "token",
      "tokenSecret"
    )
  }
}
