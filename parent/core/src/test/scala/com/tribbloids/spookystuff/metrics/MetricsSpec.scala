package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.metrics.MetricsSpec.{DummyMetrics, DummyTreeMetrics}
import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import com.tribbloids.spookystuff.relay.io.Encoder
import org.apache.spark.sql.execution.streaming.EventTimeStatsAccum
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

object MetricsSpec {

  case class DummyMetrics(
      v1: Acc[LongAccumulator] = "v1" -> 0L,
      v2: Acc[DoubleAccumulator] = "v2" -> 1.0
  ) extends AbstractMetrics

  case class DummyTreeMetrics(
      v3: Acc[EventTimeStatsAccum] = "v3" -> 2L,
      sub: DummyMetrics = DummyMetrics()
  ) extends AbstractMetrics
}

class MetricsSpec extends BaseSpec {

  TestHelper.TestSC

  it("can be converted to JSON") {
    Seq(DummyMetrics()).foreach { v =>
      val m = v.View
      m.toTreeIR
        .toJSON()
        .shouldBe(
          """
              |{
              |  "v1" : 0,
              |  "v2" : 1.0
              |}
        """.stripMargin
        )
    }
  }

  it("tree can be converted to JSON") {
    val m = DummyTreeMetrics().View_AccessorName
    m.toTreeIR
      .toJSON()
      .shouldBe(
        """
          |{
          |  "v3" : {
          |    "max" : 2,
          |    "min" : 2,
          |    "avg" : 2.0,
          |    "count" : 1
          |  },
          |  "sub" : {
          |    "v1" : 0,
          |    "v2" : 1.0
          |  }
          |}
        """.stripMargin
      )

    Encoder
      .forValue(m.toMap)
      .toJSON()
      .shouldBe(
        """
          |{
          |  "v3" : {
          |    "max" : 2,
          |    "min" : 2,
          |    "avg" : 2.0,
          |    "count" : 1
          |  },
          |  "sub/v1" : 0,
          |  "sub/v2" : 1.0
          |}
        """.stripMargin
      )
  }
}
