package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.row.PageRow

/**
 * Created by peng on 12/2/14.
 */
package object expressions {

//  private type ExpressionLike[T, +R] = T => R
//
  type Expression[+R] = ExpressionLike[PageRow, R]
  type NamedExpr[+R] = NamedExpressionLike[PageRow, R]

  type LiftedExpression[+R] = UnliftExpressionLike[PageRow, R]

//  type Alias[+R] = GenAlias[PageRow, R]

  type ByPage[+R] = (Page => R)

  type ByTrace[+R] = (Trace => R)
}