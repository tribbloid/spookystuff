package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.FetchedRow

/**
 * Created by peng on 12/2/14.
 */
package object expressions {

//  private type ExpressionLike[T, +R] = T => R
//
  type Expression[+R] = ExpressionLike[FetchedRow, R]
  type NamedExpr[+R] = NamedExpressionLike[FetchedRow, R]

  type LiftedExpression[+R] = UnliftExpressionLike[FetchedRow, R]

  type ByPage[+R] = (Doc => R)

  type ByTrace[+R] = (Trace => R)
}