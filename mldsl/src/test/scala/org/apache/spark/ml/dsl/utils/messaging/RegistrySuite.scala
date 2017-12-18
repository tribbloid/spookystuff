package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.TreeException

class RegistrySuite extends FunSpecx {

  it("findRuntimeCodec() can find Codec as companion object") {

    val v = Multipart("a", "b")()
    val codec = Registry.Default.findCodecFor(v)
    assert(codec == Multipart)

    val codec2 = Registry.Default.findCodecFor(v)
    assert(codec2 == Multipart)
  }

  it("findRuntimeCodec() will throw an exception if companion object is not a Codec") {

    val v = User("a")
    intercept[UnsupportedOperationException] {
      Registry.Default.findCodecFor(v)
    }
  }
}
