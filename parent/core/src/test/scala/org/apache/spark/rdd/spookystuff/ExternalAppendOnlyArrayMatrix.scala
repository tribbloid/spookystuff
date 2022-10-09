package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import org.scalatest.Suite

import scala.collection.immutable
import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends FunSpecx {

  override lazy val nestedSuites: immutable.IndexedSeq[Suite] = {

    immutable.IndexedSeq(
      new ExternalAppendOnlyArraySuite(
        Random.shuffle(1 to TestSC.defaultParallelism).head
      ) {},
//      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      new ExternalAppendOnlyArraySuite(
        Random.shuffle((1 + TestSC.defaultParallelism) to (TestSC.defaultParallelism * 4)).head
      ) {}
    )
  }
}
