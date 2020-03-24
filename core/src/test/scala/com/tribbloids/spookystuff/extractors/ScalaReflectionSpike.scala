package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.TestBeans.{Example, GenericExample}
import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.ml.dsl.utils.refl.{TypeUtils, UnreifiedObjectType}
import org.apache.spark.sql.types.{IntegerType, StringType}

//object ScalaReflectionSuite {

class ScalaReflectionSpike extends FunSpecx {

  import org.apache.spark.ml.dsl.utils.refl.ScalaType._

  lazy val exLit: Lit[FetchedRow, _] = Lit(new Example())
  lazy val exType: DataType = UnreifiedObjectType.summon[Example]

  it("getMethodsByName should work on overloaded function") {

    val dynamic = ScalaDynamic(
      "fn"
    )

    val paramLists = dynamic.getMethodsByCatalystType(exType).map(_.paramLists)

    paramLists
      .mkString("\n")
      .shouldBe(
        """
        |List(List(value i))
        |List()
      """.stripMargin
      )

    val returnTypes = dynamic.getMethodsByCatalystType(exType).map {
      TypeUtils.getParameter_ReturnTypes(_, exType.asTypeTag.tpe)
    }
    returnTypes
      .mkString("\n")
      .shouldBe(
        """
        |(List(List(Int)),String)
        |(List(),Int)
      """.stripMargin
      )
  }

  it("getMethodsByName should work on case constructor parameter") {

    val dynamic = ScalaDynamic(
      "a"
    )

    val paramLists = dynamic.getMethodsByCatalystType(exType).map(v => v.paramLists)

    paramLists
      .mkString("\n")
      .shouldBe(
        "List()"
      )

    val returnTypes = dynamic.getMethodsByCatalystType(exType).map {
      TypeUtils.getParameter_ReturnTypes(_, exType.asTypeTag.tpe)
    }
    returnTypes
      .mkString("\n")
      .shouldBe(
        """
        |(List(),String)
      """.stripMargin
      )
  }

  it("getMethodsByName should work on lazy val property") {

    val dynamic = ScalaDynamic(
      "c"
    )

    val paramLists = dynamic.getMethodsByCatalystType(exType).map(_.paramLists)

    paramLists
      .mkString("\n")
      .shouldBe(
        "List()"
      )

    val returnTypes = dynamic.getMethodsByCatalystType(exType).map {
      TypeUtils.getParameter_ReturnTypes(_, exType.asTypeTag.tpe)
    }
    returnTypes
      .mkString("\n")
      .shouldBe(
        """
        |(List(),String)
      """.stripMargin
      )
  }

  it("getMethodsByName should work on function with default parameters") {

    val dynamic = ScalaDynamic(
      "fnDefault"
    )

    val paramLists = dynamic.getMethodsByCatalystType(exType).map(_.paramLists)

    paramLists
      .mkString("\n")
      .shouldBe(
        "List(List(value a, value b))"
      )

    val returnTypes = dynamic.getMethodsByCatalystType(exType).map {
      TypeUtils.getParameter_ReturnTypes(_, exType.asTypeTag.tpe)
    }
    returnTypes
      .mkString("\n")
      .shouldBe(
        """
        |(List(List(Int, String)),String)
      """.stripMargin
      )
  }

  it("getMethodsByName should work on operator") {

    val dynamic = ScalaDynamic(
      "*=>"
    )

    val paramLists = dynamic.getMethodsByCatalystType(exType).map(_.paramLists)

    paramLists
      .mkString("\n")
      .shouldBe(
        "List(List(value k))"
      )

    val returnTypes = dynamic.getMethodsByCatalystType(exType).map {
      TypeUtils.getParameter_ReturnTypes(_, exType.asTypeTag.tpe)
    }
    returnTypes
      .mkString("\n")
      .shouldBe(
        """
        |(List(List(Int)),String)
      """.stripMargin
      )
  }

  it("getMethodByScala should work on overloaded function") {
    val dynamic = ScalaDynamic(
      "fn"
    )

    val paramLists = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramLists

    paramLists.toString.shouldBe(
      "List(List(value i))"
    )
  }

  it("getMethodByJava should work on overloaded function") {
    val dynamic = ScalaDynamic(
      "fn"
    )

    val paramLists = dynamic
      .getMethodByJava(exType, Some(List(IntegerType)))
      .getParameterTypes
      .mkString("|")

    paramLists.shouldBe(
      "class java.lang.Object"
    )
  }

  it("getMethodByScala should work on function with option output") {
    val dynamic = ScalaDynamic(
      "fnOpt"
    )

    val paramLists = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramLists

    paramLists.toString.shouldBe(
      "List(List(value x))"
    )
  }

  it("getMethodByJava should work on function with option output") {
    val dynamic = ScalaDynamic(
      "fnOpt"
    )

    val paramLists = dynamic.getMethodByJava(exType, Some(List(IntegerType))).getParameterTypes.mkString("|")

    paramLists.shouldBe(
      "class java.lang.Object"
    )
  }

  ignore("getMethodByScala should work on function with option parameter") {
    val dynamic = ScalaDynamic(
      "fnOptOpt"
    )

    val paramLists = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramLists

    paramLists.toString.shouldBe(
      "List(List(value x))"
    )
  }

  ignore("getMethodByJava should work on function with option parameter") {
    val dynamic = ScalaDynamic(
      "fnOptOpt"
    )

    val paramLists = dynamic.getMethodByJava(exType, Some(List(IntegerType))).getParameterTypes.mkString("|")

    paramLists.shouldBe(
      "class scala.Option"
    )
  }

  it("getMethodByScala should throw error if parameter Type is incorrect") {
    val dynamic = ScalaDynamic(
      "fn"
    )

    intercept[UnsupportedOperationException] {
      val paramLists = dynamic.getMethodByScala(exType, Some(List(StringType)))
    }
  }

  //TODO: this doesn't matter as its only used after scala reflection-based method, but should be fixed in the future
  ignore("getMethodByJava should return None if parameter Type is incorrect") {
    val dynamic = ScalaDynamic(
      "fn"
    )

    intercept[UnsupportedOperationException] {
      val paramLists = dynamic.getMethodByJava(exType, Some(List(StringType)))
    }
  }

  it("getMethodByScala should work on operator") {

    val dynamic = ScalaDynamic(
      "*=>"
    )

    val method = dynamic.getMethodByScala(exType, Some(List(IntegerType)))
    val paramLists = method.paramLists

    paramLists.toString.shouldBe(
      "List(List(value k))"
    )

    val returnType = TypeUtils.getParameter_ReturnTypes(method, exType.asTypeTag.tpe)
    returnType.toString.shouldBe(
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  it("getMethodByJava should work on operator") {

    val dynamic = ScalaDynamic(
      "*=>"
    )

    val method = dynamic.getMethodByJava(exType, Some(List(IntegerType)))
    val types = method.getParameterTypes.mkString("|")

    types.toString.shouldBe(
      "class java.lang.Object"
    )
  }

  //  test("resolveType should work on ")

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

class ScalaReflectionSpike_Generic extends ScalaReflectionSpike {

  override lazy val exLit = Lit(new GenericExample[Int]("dummy", 1))
  //  val evi = (ex.dataType)
  override lazy val exType = UnreifiedObjectType.summon[GenericExample[Int]]
}
