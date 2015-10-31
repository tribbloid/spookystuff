package com.tribbloids.spookystuff.pipeline

import java.util.UUID

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import org.apache.spark.ml.param.ParamMap

/**
 * Created by peng on 31/10/15.
 */
class TransformerChain(
                        self: Seq[SpookyTransformer],
                        override val uid: String =
                        classOf[TransformerChain].getCanonicalName + "_" + UUID.randomUUID().toString
                        ) extends SpookyTransformerLike {

  //this is mandatory for Params.defaultCopy()
  def this(uid: String) = this(Nil, uid)

  override def transform(dataset: PageRowRDD): PageRowRDD = self.foldLeft(dataset) {
    (rdd, transformer) =>
      transformer.transform(rdd)
  }

  override def copy(extra: ParamMap): TransformerChain = new TransformerChain(
    self = this
      .self
      .map(_.copy(extra)),
    uid = this.uid
  )

  def +> (another: SpookyTransformer): TransformerChain = new TransformerChain(
    this.self :+ another,
    uid = this.uid
  )

  override def test(spooky: SpookyContext): Unit = self.foreach(_.test(spooky))
}
