package org.apache.spark.rdd.spookystuff

import org.scalatest.{FunSpec, Suite}

import scala.collection.immutable
import scala.util.Random

class ExternalAppendOnlyArrayMatrix extends FunSpec {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  val p1: Int = Random.shuffle(1 to TestSC.defaultParallelism).head
  val p2: Int = Random.shuffle((1 + TestSC.defaultParallelism) to (TestSC.defaultParallelism * 4)).head

  override val nestedSuites: immutable.IndexedSeq[Suite] = {

    immutable.IndexedSeq(
      ExternalAppendOnlyArraySuite(p1),
//      ExternalAppendOnlyArraySuite(TestSC.defaultParallelism),
      ExternalAppendOnlyArraySuite(p2)
    )
  }
}
