package org.tribbloid.spookystuff.entity.clientaction

import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Http client operations that doesn't require a browser
 * e.g. wget, restful API invocation
 */
trait Sessionless extends ClientAction {

  override final def doExe(pb: PageBuilder): Array[Page] = this.exeWithoutSession

  def exeWithoutSession: Array[Page]
}
