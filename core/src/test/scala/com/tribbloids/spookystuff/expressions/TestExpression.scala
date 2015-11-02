package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.row.{Key, PageRow}
import org.scalatest.FunSuite

import scala.collection.immutable.ListMap

/**
 * Created by peng on 01/11/15.
 */
class TestExpression extends FunSuite {

  import com.tribbloids.spookystuff.dsl._

  test("Dynamic functions should be applicable on values") {
    val pageRow = PageRow(ListMap(Key("K1") -> "a,b,c", Key("K2") -> 2))
    val afterDynamic: Expression[Any] = dynamic('K1).split(",")
    val afterDynamicValue: Option[Any] = afterDynamic.apply(pageRow)
    assert(afterDynamicValue.get.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  }

  test("Dynamic functions should be applicable on expressions") {
    val pageRow = PageRow(ListMap(Key("K1") -> "a,b,c", Key("K2") -> ","))
    val afterDynamic: Expression[Any] = dynamic('K1).split(dynamic('K2))
    val afterDynamicValue: Option[Any] = afterDynamic.apply(pageRow)
    assert(afterDynamicValue.get.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  }
}
