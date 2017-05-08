package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.FetchedRow
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

package object dsl extends DSL {

  type ByDoc[+R] = (Doc => R)
  type ByTrace[+R] = (Trace => R)
}