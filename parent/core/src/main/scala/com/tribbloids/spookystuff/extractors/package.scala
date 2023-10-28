package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.row.FetchedRow

/**
  * Created by peng on 12/2/14.
  */
package object extractors {

  type DataType = org.apache.spark.sql.types.DataType

  type FR = FetchedRow

  type Extractor[+R] = GenExtractor[FR, R]

  type Expr[+R] = GenExpr[FR, R]
  def Expr: GenExpr.type = GenExpr

}
