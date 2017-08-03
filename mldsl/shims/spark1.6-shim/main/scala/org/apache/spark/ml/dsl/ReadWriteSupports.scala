package org.apache.spark.ml.dsl

import org.apache.spark.ml.Pipeline.SharedReadWrite
import org.apache.spark.ml.dsl.utils.{FlowRelay, MessageMLReader, MessageMLWriter, MessageRelay}
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable}

/**
  * Created by peng on 05/10/16.
  */
object ReadWriteSupports {

  implicit class FlowReadable(flow: Flow) extends MLReadable[Flow] {

    object FlowReader extends MLReader[Flow] {

      /** Checked against metadata when loading model */
      private val className = classOf[Flow].getName

      override def load(path: String): Flow = {
        val (
          uid,
          stages
          ) = SharedReadWrite.load(className, this.sc, path) //TODO: not sure if it can be reused

        null
      }
    }

    override def read = FlowRelay.toMLReader
  }

  implicit class FlowWritable(flow: Flow) extends MLWritable {

    override def write = {
      flow.propagateCols(Flow.DEFAULT_COMPACTION)

      FlowRelay.toMLWriter(flow)
    }
  }

  implicit class MessageRelayReadWrite[Obj](mr: MessageRelay[Obj]) {

    final def toMLReader: MLReader[Obj] = MessageMLReader(mr)

    final def toMLWriter(v: Obj) = MessageMLWriter(mr.toMessageAPI(v))
  }
}
