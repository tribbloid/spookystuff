package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import org.apache.spark.ml.dsl.utils.refl.UnknownDT
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

/**
  * Created by peng on 17/05/16.
  */
class TestFlattenPlan extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.dsl._

  lazy val df = sql.createDataFrame(Seq(Seq(1, 2, 3) -> null, Seq(4, 5, 6) -> Seq("b", "c", "d")))
  lazy val src = spooky.create(df)

  it("FlattenPlan should work on collection") {
    val rdd1 = src
      .flatten('_1 ~ 'B)
      .toMapRDD(true)

    rdd1
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |Map(_1 -> ArraySeq(1, 2, 3), B -> 1)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), B -> 4)
        |Map(_1 -> ArraySeq(1, 2, 3), B -> 2)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), B -> 5)
        |Map(_1 -> ArraySeq(1, 2, 3), B -> 3)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), B -> 6)
      """.stripMargin,
        sort = true
      )
  }

  it("FlattenPlan should work on collection if overwriting defaultJoinField") {
    val rdd1 = src
      .flatten('_1 ~ 'A)
      .toMapRDD(true)

    rdd1
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |Map(_1 -> ArraySeq(1, 2, 3), A -> 1)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> 4)
        |Map(_1 -> ArraySeq(1, 2, 3), A -> 2)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> 5)
        |Map(_1 -> ArraySeq(1, 2, 3), A -> 3)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> 6)
      """.stripMargin,
        sort = true
      )
  }

  it("FlattenPlan should work on collection if not manually set alias") {
    val rdd1 = src
      .flatten('_1)
      .toMapRDD(true)

    rdd1
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |Map(_1 -> 1)
        |Map(_2 -> ArraySeq(b, c, d), _1 -> 4)
        |Map(_1 -> 2)
        |Map(_2 -> ArraySeq(b, c, d), _1 -> 5)
        |Map(_1 -> 3)
        |Map(_2 -> ArraySeq(b, c, d), _1 -> 6)
      """.stripMargin,
        sort = true
      )
  }

  it("FlattenPlan should work on partial collection") {
    val rdd1 = src
      .flatten('_2 ~ 'A)
      .toMapRDD(true)

    rdd1
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |Map(_1 -> ArraySeq(1, 2, 3))
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> b)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> c)
        |Map(_1 -> ArraySeq(4, 5, 6), _2 -> ArraySeq(b, c, d), A -> d)
      """.stripMargin,
        sort = true
      )
  }

  def assertTypeEqual(t1: UnknownDT[_], t2: UnknownDT[_]) = {
    assert(t1.typeMagnet.asClass == t2.typeMagnet.asClass)
    assert(t1.typeMagnet.asType =:= t2.typeMagnet.asType)
    assert(t1 == t2)
  }

  it("FlattenPlan should work on extracted array") {
    val extracted = src
      .wget(
        HTML_URL
      )
      .extract(
        Lit(Array("a" -> 1, "b" -> 2)) ~ 'Array
      )

    val t1 = extracted.schema.getReference('Array).get.dataType
//    val t2 = UnknownDT.summon[Array[Tuple2[String, Int]]].asInstanceOf[UnknownDT[_]]

    assert(
      t1 == ArrayType(
        StructType(
          Array(
            StructField("_1", StringType, nullable = true),
            StructField("_2", IntegerType, nullable = false)
          )
        )
      )
    )
//    assertTypeEqual(t1, t2)

    val flattened = extracted
      .flatten(
        'Array
      )

    //    assert(flattened.schema.typedFor('Array).get.dataType == UnreifiedScalaType.apply[Tuple2[String, Int]])
    assert(
      flattened.schema.getReference('Array).get.dataType == StructType(
        Array(
          StructField("_1", StringType, nullable = true),
          StructField("_2", IntegerType, nullable = false)
        )
      )
    )
  }

  it("FlattenPlan should work on extracted Seq") {
    val extracted = src
      .wget(
        HTML_URL
      )
      .extract(
        Lit(Seq("a" -> 1, "b" -> 2)) ~ 'Array
      )

    val t1 = extracted.schema.getReference('Array).get.dataType
    assert(
      t1 == ArrayType(
        StructType(
          Array(
            StructField("_1", StringType, nullable = true),
            StructField("_2", IntegerType, nullable = false)
          )
        )
      )
    )

    val flattened = extracted
      .flatten(
        'Array
      )

    //    assert(flattened.schema.typedFor('Array).get.dataType == UnreifiedScalaType.apply[Tuple2[String, Int]])
    assert(
      flattened.schema.getReference('Array).get.dataType == StructType(
        Array(
          StructField("_1", StringType, nullable = true),
          StructField("_2", IntegerType, nullable = false)
        )
      )
    )
  }

  it("FlattenPlan should work on extracted List") {
    val extracted = src
      .wget(
        HTML_URL
      )
      .extract(
        Lit(List("a" -> 1, "b" -> 2)) ~ 'Array
      )

    val t1 = extracted.schema.getReference('Array).get.dataType
    assert(
      t1 == ArrayType(
        StructType(
          Array(
            StructField("_1", StringType, nullable = true),
            StructField("_2", IntegerType, nullable = false)
          )
        )
      )
    )

    val flattened = extracted
      .flatten(
        'Array
      )

    //    assert(flattened.schema.typedFor('Array).get.dataType == UnreifiedScalaType.apply[Tuple2[String, Int]])
    assert(
      flattened.schema.getReference('Array).get.dataType == StructType(
        Array(
          StructField("_1", StringType, nullable = true),
          StructField("_2", IntegerType, nullable = false)
        )
      )
    )
  }

  it("flatExtract is equivalent to flatten + extract") {
    val rdd1 = src
      .flatExtract('_2 ~ 'A)(
        'A ~ 'dummy
      )

    val rdd2 = src
      .flatten('_2 ~ 'A)
      .extract(
        'A ~ 'dummy
      )

    rdd1
      .toMapRDD(true)
      .collect()
      .mkString("\n")
      .shouldBe(
        rdd2.toMapRDD(true).collect().mkString("\n")
      )
    rdd1
      .toDF(sort = true)
      .collect()
      .mkString("\n")
      .shouldBe(
        rdd2.toDF(sort = true).collect().mkString("\n")
      )
  }

  it("flatExtract is equivalent to flatten + extract if not manually set join key") {
    val rdd1 = src
      .flatExtract('_2)(
        'A ~ 'dummy
      )

    val rdd2 = src
      .flatten('_2 ~ 'A.*)
      .extract(
        'A ~ 'dummy
      )

    rdd1
      .toMapRDD(true)
      .collect()
      .mkString("\n")
      .shouldBe(
        rdd2.toMapRDD(true).collect().mkString("\n")
      )
    rdd1
      .toDF(sort = true)
      .collect()
      .mkString("\n")
      .shouldBe(
        rdd2.toDF(sort = true).collect().mkString("\n")
      )
  }

  //  test("describe ACF") {
  //    val doc =
  //
  //    val df = doc.extract(
  //      S"dataAsset" ~ 'asset,
  //      x"${S"hierarchyOwner > organization".text}/${S"hierarchyOwner > businessUnit".text}/${S"hierarchyOwner > group".text}" ~ 'alias
  //    )
  //      .extract(
  //        'asset.typed[Elements[Unstructured]].andThen {
  //          vv =>
  //            val seq: Seq[(String, (String, String))] = vv.map{
  //              v =>
  //                (v\"assetCode" text).get ->
  //                  ((v\"assetName" text).get -> (v\"parentAssetCode" text).get)
  //            }
  //
  //            val map: Map[String, (String, String)] = Map(seq: _*)
  //            expandAssetNames(map)
  //        } ~ 'code_path
  //      )
  //      .remove('asset)
  //      .flatten('code_path)
  //      .extract(
  //        'code_path.typed[(String, String)].andThen(_._1) as 'code,
  //        'code_path.typed[(String, String)].andThen(_._2) as 'path
  //      )
  //      .toDF(sort = true)
  //  }
}
