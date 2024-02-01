package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.utils.collection.BacktrackingIterator

import scala.util.Random

class BacktrackingIteratorSuite extends BaseSpec {

  it("can backtrack once") {

    val base = (0 until 100).iterator

    val itr = BacktrackingIterator(base)

    for (i <- 1 to 10) itr.next()
    val id = itr.snapshot()
    assert(id == 0)
    for (i <- 1 to 10) itr.next()

    assert(itr.next() == 20)
    itr.revert(0)
    assert(itr.next() == 10)

    assert(itr.toSeq == (11 until 100))
  }

  it("can backtrack for arbitrary times") {

    val base = (0 until 100).iterator
    val itr = BacktrackingIterator(base)

    val id_expected = for (i <- 0 until 3) yield {

      val rnd = Random.nextInt(30)

      for (i <- 1 to rnd) itr.next()
      val id = itr.snapshot()
      assert(i == id)

      i -> itr.next()
    }

    id_expected.foreach {
      case (_id, expected) =>
        itr.revert(_id)
        assert(itr.toSeq == (expected until 100))
    }
  }
}
