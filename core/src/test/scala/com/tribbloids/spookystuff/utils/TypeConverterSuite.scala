package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.{Action, ActionUDT}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

/**
  * Created by peng on 08/06/16.
  */
class TypeConverterSuite extends SpookyEnvSuite {

  import ScalaReflection.universe._
  import ImplicitUtils._

  /**
    * please keep this test to quickly identify any potential problems caused by changes in scala reflection API in the future
    */
  test("scala reflection can be used to get type of Array[String].headOption") {

    val arr: Seq[String] = Seq("abc", "def")
    val cls = arr.head.getClass
    val ttg: TypeTag[Seq[String]] = TypeUtils.instanceToTypeTag(arr)
    val fns = ttg.tpe
      .members
    val fn = fns
      .filter(_.name.toString == "head")
      .head                           // Unsafely access it for now, use Option and map under normal conditions

    val fnTp: Type = fn.typeSignatureIn(ttg.tpe)

    val clsTp = fnTp.typeSymbol.asClass
    val fnRetTp = fnTp.asInstanceOf[ScalaReflection.universe.NullaryMethodType].resultType

    val fnCls = ttg.mirror.runtimeClass(fnRetTp)
    val fnCls2 = ttg.mirror.runtimeClass(clsTp)

    assert(cls == fnCls)
    assert(cls == fnCls2)
  }

  //  test("atomicTypePairs works as intended") {
  //    TypeUtils.atomicTypePairs.foreach(println)
  //  }

  val tupleSchema = StructType(Array(StructField("_1", IntegerType, nullable = false), StructField("_2", StringType, nullable = true)))

  val typePairs: Seq[(DataType, TypeTag[_])] = Seq(
    StringType -> typeTag[String],
    IntegerType -> typeTag[Int],
    DoubleType -> typeTag[Double],
    ArrayType(DoubleType, containsNull = false) -> typeTag[Array[Double]],
    //    ArrayType(DoubleType, containsNull = true) -> typeTag[Array[Option[Double]]],
    ArrayType(StringType, containsNull = true) -> typeTag[Array[String]],
    //    ArrayType(StringType, containsNull = true) -> typeTag[Array[(String, Int)]],

    //    tupleSchema -> typeTag[(Int, String)], //TODO: urge spark team to fix the bug and re-enable it
    new ActionUDT -> typeTag[Action]
  )

  typePairs.foreach{
    pair =>
      test(s"scalaType (${pair._2.tpe}) => catalystType (${pair._1})") {
        val converted = pair._2.catalystTypeOpt
        println(converted)
        assert(converted == Some(pair._1))
      }

      test(s"catalystType (${pair._1}) => scalaType (${pair._2.tpe})") {
        val converted = pair._1.scalaTypeOpt
        println(converted)
        assert(converted.map(_.toClass) == Some(pair._2.toClass))
      }
  }

  val oneWayPairs: Seq[(TypeTag[_], DataType)] = Seq(
    typeTag[Array[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true),
    typeTag[Seq[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true)
//    typeTag[Set[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true),
//    typeTag[Iterable[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true)
  )

  oneWayPairs.foreach{
    pair =>
      test(s"scalaType (${pair._1.tpe}) => catalystType (${pair._2})") {
        val converted = pair._1.catalystTypeOpt
        println(converted)
        assert(converted == Some(pair._2))
      }
  }
}