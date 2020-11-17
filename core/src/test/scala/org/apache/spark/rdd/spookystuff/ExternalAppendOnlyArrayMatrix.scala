package org.apache.spark.rdd.spookystuff

import org.scalatest.{FunSpec, Suite}

import scala.collection.immutable
import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends FunSpec {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  override val nestedSuites: immutable.IndexedSeq[Suite] = {

    immutable.IndexedSeq(
      ExternalAppendOnlyArraySuite(Random.nextInt(TestSC.defaultParallelism - 2) + 2),
//      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      ExternalAppendOnlyArraySuite(Random.nextInt(TestSC.defaultParallelism - 1) + 1 + TestSC.defaultParallelism)
    )
  }
}
