package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.scalatest.Suite

import scala.collection.immutable
import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends FunSpecx {

  import ExternalAppendOnlyArrayMatrix._

  override val nestedSuites: immutable.IndexedSeq[Suite] = {

    immutable.IndexedSeq(
      S1,
//      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      S2
    )
  }
}

object ExternalAppendOnlyArrayMatrix {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  object S1
      extends ExternalAppendOnlyArraySuite(
        Random.shuffle(1 to TestSC.defaultParallelism).head
      )

  object S2
      extends ExternalAppendOnlyArraySuite(
        Random.shuffle((1 + TestSC.defaultParallelism) to (TestSC.defaultParallelism * 4)).head
      )
}
