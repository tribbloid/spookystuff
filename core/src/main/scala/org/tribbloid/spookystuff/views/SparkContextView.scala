package org.tribbloid.spookystuff.views

import org.apache.spark.SparkContext
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.sparkbinding.PageRowRDD

/**
 * Created by peng on 12/28/14.
 */
class SparkContextView(self: SparkContext) {

  def noInput = self.parallelize(Seq(PageRow()))

}
