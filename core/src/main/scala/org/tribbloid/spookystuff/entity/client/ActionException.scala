package org.tribbloid.spookystuff.entity.client

import org.tribbloid.spookystuff.SpookyException

/**
 * Created by peng on 9/11/14.
 */
class ActionException(
                             override val message: String = "",
                             override val cause: Throwable = null
                             ) extends SpookyException(message, cause)
//
//class InteractionException(
//                             override val message: String = "",
//                             override val cause: Throwable = null
//                             ) extends ActionException(message, cause)
//
//class ExportException(
//                            override val message: String = "",
//                            override val cause: Throwable = null
//                            ) extends ActionException(message, cause)