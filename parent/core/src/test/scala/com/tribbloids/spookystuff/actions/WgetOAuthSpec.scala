package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.agent.{Agent, OAuthKeys}
import com.tribbloids.spookystuff.conf.Core
import org.scalatest.tags.Retryable

/**
  * Adding OAuth parameters should not affect results of other queries
  */
@Retryable
class WgetOAuthSpec extends WgetSpec {

  override def wget(uri: String): Action = {
    val action: OAuthV2 = OAuthV2(Wget(uri))
    val session = new Agent(spooky)
    val effective = action.rewrite(session)
    assert(effective.uri !== uri)
    action
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spooky(Core).confUpdate { v =>
      val result = v.copy(
        oAuthKeysFactory = Impl(_ =>
          OAuthKeys(
            "consumerKey",
            "consumerSecret",
            "token",
            "tokenSecret"
          )
        )
      )
      result
    }
  }
}
