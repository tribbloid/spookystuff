package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.TupleX.T0
import ai.acyclic.prover.commons.testlib.BaseSpec
import com.tribbloids.spookystuff.linq.Foundation.^

import scala.collection.immutable.ListMap

class FieldAccessorSpec extends BaseSpec {

  it("value") {

    val t1 = ^(x = 1, y = "ab")
    val col = FieldAccessor(t1).y

    assert(col.value == "ab")
  }

  it("as typed Row") {

    val t1 = ^(x = 1, y = "ab")
    val col = FieldAccessor(t1).y

    val colAsRow = col.asRow
    val colAsRowGT = ^(y = "ab")

    implicitly[colAsRow._internal.Repr =:= colAsRowGT._internal.Repr]
  }

  it("update") {

    val t1 = ^(x = 1, y = "ab")
    val col = FieldAccessor(t1).y

    val t2 = col := 1.0
    val t2GT = t1 +<+ ^(y = 1.0)

    implicitly[t2._internal.Repr =:= t2GT._internal.Repr]

    assert(t2GT._internal.runtimeMap == ListMap('x -> 1, 'y -> 1.0))
    assert(t2.y == 1.0)
  }

  it("remove") {

    val t1 = ^(x = 1, y = "ab")
    val col = FieldAccessor(t1).y

    val removedAsTuple = col.remove.asTuple()

    assert(removedAsTuple == "ab" -> 1 *: T0)

    val removed = col.remove()

    assert(removed._internal.runtimeMap == ListMap('x -> 1))
    assert(removed.x == 1)
  }
}
