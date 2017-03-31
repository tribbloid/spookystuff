package com.tribbloids.spookystuff.pipeline

import java.util.UUID

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.rdd.FetchedDataset
import org.apache.spark.ml.param.ParamMap

/**
 * Created by peng on 31/10/15.
 */
class RemoteTransformerChain(
                        self: Seq[RemoteTransformer],
                        override val uid: String =
                        classOf[RemoteTransformerChain].getCanonicalName + "_" + UUID.randomUUID().toString
                        ) extends RemoteTransformerLike {

  //this is mandatory for Params.defaultCopy()
  def this(uid: String) = this(Nil, uid)

  override def transform(dataset: FetchedDataset): FetchedDataset = self.foldLeft(dataset) {
    (rdd, transformer) =>
      transformer.transform(rdd)
  }

  override def copy(extra: ParamMap): RemoteTransformerChain = new RemoteTransformerChain(
    self = this
      .self
      .map(_.copy(extra)),
    uid = this.uid
  )

  def +> (another: RemoteTransformer): RemoteTransformerChain = new RemoteTransformerChain(
    this.self :+ another,
    uid = this.uid
  )

  override def test(spooky: SpookyContext): Unit = self.foreach(_.test(spooky))
}
