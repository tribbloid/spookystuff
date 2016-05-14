package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.pages.Fetched
import com.tribbloids.spookystuff.row.{DataRow, Field}
import org.scalatest.FunSuite

import scala.collection.immutable.ListMap

/**
 * Created by peng on 01/11/15.
 */
class TestExpression extends FunSuite {

  import com.tribbloids.spookystuff.dsl._

  test("Dynamic functions should be applicable on values") {
    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> 2))
    val pageRow = dataRow -> Seq[Fetched]()

    assert(dynamic('K1).isDefinedAt(pageRow))
    assert(dynamic('K1).apply(pageRow) == "a,b,c")
    val afterDynamic: Expression[Any] = dynamic('K1).split(",")
    val afterDynamicValue = afterDynamic.apply(pageRow)
    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  }

  test("Dynamic functions should be applicable on expressions") {
    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> ","))
    val pageRow = dataRow -> Seq[Fetched]()
    val afterDynamic: Expression[Any] = dynamic('K1).split(dynamic('K2))
    val afterDynamicValue = afterDynamic.apply(pageRow)
    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  }
}
