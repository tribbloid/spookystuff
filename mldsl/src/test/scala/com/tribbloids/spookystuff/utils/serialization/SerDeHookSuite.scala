package com.tribbloids.spookystuff.utils.serialization
import com.tribbloids.spookystuff.testutils.FunSpecx
import org.scalatest.BeforeAndAfter

object SerDeHookSuite {

  var readCounter = 0
  var writeCounter = 0

  val example: SerDeHook = SerDeHook(
    _ => readCounter += 1,
    _ => writeCounter += 1
  )
}

class SerDeHookSuite extends FunSpecx with BeforeAndAfter {

  import SerDeHookSuite._

  before {
    readCounter = 0
    writeCounter = 0
  }

  it("can be triggered without affecting default SerDe") {

    AssertWeaklySerializable(
      example
    )

    assert(readCounter == 2)
    assert(writeCounter == 2)
  }
}
