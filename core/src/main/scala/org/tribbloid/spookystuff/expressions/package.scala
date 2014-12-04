package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.entity.PageRow

/**
 * Created by peng on 12/2/14.
 */
package object expressions {

  type Expr[+T] = NamedFunction1[PageRow, Option[T]]
}