package com.tribbloids.spookystuff.utils.serialization

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.atomic.AtomicInteger

class BeforeAndAfterShippingSuite extends BaseSpec with BeforeAndAfterEach {

  import BeforeAndAfterShippingSuite._

  override def beforeEach(): Unit = {
    beforeCounter.set(0)
    afterCounter.set(0)
  }

  it("can serialize container") {
    val dummy = Dummy()

    AssertSerializable(dummy.forShipping)

    assert(beforeCounter.get() == 2)
    assert(afterCounter.get() == 2)
  }

  ignore("can serialize self") {
    // TODO: this doesn't work, why?
//  it("can serialize self") {

    val dummy = Dummy()

    AssertSerializable(dummy)

    assert(beforeCounter.get() == 2)
    assert(afterCounter.get() == 2)
  }
}

object BeforeAndAfterShippingSuite {

  val beforeCounter: AtomicInteger = new AtomicInteger(0)
  val afterCounter: AtomicInteger = new AtomicInteger(0)

  case class Dummy(
      i: Int = 1,
      j: Double = 2.375,
      s: String = "a"
  ) extends BeforeAndAfterShipping {

    val s2: String = "b"

    override def beforeDeparture(): Unit = {
      assert(s2 == "b")
      beforeCounter.incrementAndGet()
    }

    override def afterArrival(): Unit = {
      assert(s2 == "b") // ensure that Dummy is fully initialised when doAfter is called
      afterCounter.incrementAndGet()
    }
  }
}
