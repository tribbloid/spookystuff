package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

import scala.collection.mutable.ArrayBuffer

class HasEagerObjectsSuite extends FunSpecx with HasEager {

  val initialized: ArrayBuffer[Class[_]] = ArrayBuffer.empty

  reifyEager()

  trait HasInitCount {
    initialized += this.getClass
  }

  object _eager extends HasInitCount with Eager

  object _notEager extends HasInitCount

  it("object that extends Eager should be initialized") {
    assert(initialized.size == 1)

    assert(initialized.contains(_eager.getClass))
  }
}
