package org.tribbloid.spookystuff.entity.clientaction

import org.tribbloid.spookystuff.Const

/**
 * Created by peng on 9/10/14.
 */
trait Timed extends ClientAction {

  val timeout: Int = Const.resourceTimeout
}
