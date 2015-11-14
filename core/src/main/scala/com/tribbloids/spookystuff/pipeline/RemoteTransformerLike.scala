package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import org.apache.spark.ml.param.{ParamMap, Params}

/**
 * Created by peng on 25/09/15.
 */
private[pipeline] trait RemoteTransformerLike extends Params with Serializable {

  def transform(dataset: PageRowRDD): PageRowRDD

  def copy(extra: ParamMap): RemoteTransformerLike = this.defaultCopy(extra)

  def +> (another: RemoteTransformer): RemoteTransformerChain

  def test(spooky: SpookyContext): Unit
}
