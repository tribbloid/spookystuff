package com.tribbloids.spookystuff.unused

import ai.acyclic.prover.commons.spark.TestHelper.TestSC
import com.tribbloids.spookystuff.testutils.BaseSpec
import org.scalatest.Suite

import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends BaseSpec {

  val p1: Int = Random.shuffle(1 to TestSC.defaultParallelism).head
  val p2: Int = Random.shuffle((1 + TestSC.defaultParallelism) to (TestSC.defaultParallelism * 4)).head

  override val nestedSuites: Vector[Suite] = {

    Vector(
      new ExternalAppendOnlyArraySuite(p1) {},
      //      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      new ExternalAppendOnlyArraySuite(p2) {}
    )
  }
}
