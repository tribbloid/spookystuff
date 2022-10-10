package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.scalatest.Suite

import scala.collection.immutable
import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends FunSpecx {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  val p1: Int = Random.shuffle(1 to TestSC.defaultParallelism).head
  val p2: Int = Random.shuffle((1 + TestSC.defaultParallelism) to (TestSC.defaultParallelism * 4)).head

  override val nestedSuites: immutable.IndexedSeq[Suite] = {

    immutable.IndexedSeq(
      new ExternalAppendOnlyArraySuite(p1) {},
      //      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      new ExternalAppendOnlyArraySuite(p2) {}
    )
  }
}
