package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.FetchedRow

/**
 * Created by peng on 12/2/14.
 */
package object extractors {

//  private type ExpressionLike[T, +R] = T => R

  type Extractor[+R] = GenExtractor[FetchedRow, R]
  type NamedExtr[+R] = NamedGenExtractor[FetchedRow, R]

  type UnliftedExtr[+R] = UnliftedGenExtractor[FetchedRow, R]

  type ByDoc[+R] = (Doc => R)
  type ByTrace[+R] = (Trace => R)
}