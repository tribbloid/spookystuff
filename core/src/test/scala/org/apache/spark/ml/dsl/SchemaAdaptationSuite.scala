package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.FlowUtils

/**
  * Created by peng on 09/05/16.
  */
class SchemaAdaptationSuite extends AbstractFlowSuite {

  test("cartesianProduct should work on list") {
    val before: List[List[String]] = List(
      List("a", "b", "c"),
      List("1", "2", "3"),
      List("x", "y", "z")
    )
    val after = FlowUtils.cartesianProductList(before)

    after.mkString("\n").shouldBe(
      """
        |List(a, 1, x)
        |List(a, 1, y)
        |List(a, 1, z)
        |List(a, 2, x)
        |List(a, 2, y)
        |List(a, 2, z)
        |List(a, 3, x)
        |List(a, 3, y)
        |List(a, 3, z)
        |List(b, 1, x)
        |List(b, 1, y)
        |List(b, 1, z)
        |List(b, 2, x)
        |List(b, 2, y)
        |List(b, 2, z)
        |List(b, 3, x)
        |List(b, 3, y)
        |List(b, 3, z)
        |List(c, 1, x)
        |List(c, 1, y)
        |List(c, 1, z)
        |List(c, 2, x)
        |List(c, 2, y)
        |List(c, 2, z)
        |List(c, 3, x)
        |List(c, 3, y)
        |List(c, 3, z)
      """.stripMargin
    )
  }

  test("cartesianProduct should work on empty list") {
    val before: List[Set[String]] = List()
    val after = FlowUtils.cartesianProductSet(before).toList.sortBy(_.toString())

    after.mkString("\n").shouldBe(
      """
        |List()
      """.stripMargin
    )
  }

  test("cartesianProduct should work on list of empty sets") {
    val before: List[Set[String]] = List(
      Set(),
      Set("1", "2", "3"),
      Set()
    )
    val after = FlowUtils.cartesianProductSet(before).toList.sortBy(_.toString())

    after.mkString("\n").shouldBe("")
  }
}
