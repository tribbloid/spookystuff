package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.pages.PageUID
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap

/**
 * Created by peng on 2/21/15.
 */
package object row {

  type SortKey = Key with SortKeyMixin

  type HiddenKey = Key with HiddenKeyMixin

  type DataRow =  ListMap[KeyLike, Any]

  type SegID = Long
  type RowUID = (Seq[PageUID], SegID)

  type SquashedRow = Squashed[PageRow]

  type WebCacheRow = (DryRun, SquashedRow)

  type WebCacheRDD = RDD[WebCacheRow]
}
