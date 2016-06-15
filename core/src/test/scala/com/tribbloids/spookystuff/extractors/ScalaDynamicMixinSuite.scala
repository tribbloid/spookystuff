package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvSuite

/**
  * Created by peng on 15/06/16.
  */
class ScalaDynamicMixinSuite extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  //  test("Dynamic functions should be applicable on values") {
  //    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> 2))
  //    val pageRow = FetchedRow(dataRow, Seq[Fetched]())
  //
  //    assert(dynamic('K1).isDefinedAt(pageRow))
  //    assert(dynamic('K1).apply(pageRow) == "a,b,c")
  //    val afterDynamic: Extractor[Any] = dynamic('K1).split(",")
  //    val afterDynamicValue = afterDynamic.apply(pageRow)
  //    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  //  }
  //
  //  test("Dynamic functions should be applicable on expressions") {
  //    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> ","))
  //    val pageRow = dataRow -> Seq[Fetched]()
  //    val afterDynamic: Extractor[Any] = dynamic('K1).split(dynamic('K2))
  //    val afterDynamicValue = afterDynamic.apply(pageRow)
  //    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  //  }
}
