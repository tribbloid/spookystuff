package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.ml.dsl.utils.messaging.TestBeans._

class CodecRegistrySuite extends FunSpecx {

  it("findRuntimeCodec() can find Codec as companion object") {

    val v = Multipart("a", "b")()
    val codec = RelayRegistry.Default.findCodecFor(v)
    assert(codec == Multipart)

    val codec2 = RelayRegistry.Default.findCodecFor(v)
    assert(codec2 == Multipart)
  }

  it("findRuntimeCodec() will throw an exception if companion object is not a Codec") {

    val v = User("a")
    intercept[UnsupportedOperationException] {
      RelayRegistry.Default.findCodecFor(v)
    }
  }
}
