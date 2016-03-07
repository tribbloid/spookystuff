package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.expressions.ExpressionLike
import com.tribbloids.spookystuff.row.PageRow
import com.tribbloids.spookystuff.pages.Page

/**
 * Created by peng on 12/2/14.
 */
package object expressions {

//  trait Expression[+R] extends ExpressionLike[PageRow, Option[R]]

  type Expression[+R] = ExpressionLike[PageRow, Option[R]]

  type ByPage[+R] = (Page => R)

  type ByTrace[+R] = (Trace => R)
}