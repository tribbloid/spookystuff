package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import org.apache.spark.ml.param.{ParamMap, Params}

/**
 * Created by peng on 25/09/15.
 */
private[pipeline] trait SpookyTransformerLike extends Params with Serializable {

  def transform(dataset: PageRowRDD): PageRowRDD

  def copy(extra: ParamMap): SpookyTransformerLike = this.defaultCopy(extra)

  def +> (another: SpookyTransformer): TransformerChain

  def test(spooky: SpookyContext): Unit
}
