package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.entity.PageRow
import com.tribbloids.spookystuff.pages.Page

/**
 * Created by peng on 12/2/14.
 */
package object expressions {

  type Expression[+R] = NamedFunction1[PageRow, Option[R]]

  type ForceExpression[+R] = ForceNamedFunction1[PageRow, Option[R]]

  type PageFilePath[+R] = (Page => R)

  type CacheFilePath[+R] = (Trace => R)
}