package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.actions.{Action, ActionUDT, Wget}
import com.tribbloids.spookystuff.doc.{Doc, DocOption, Unstructured}
import com.tribbloids.spookystuff.extractors.impl.{Get, Lit}
import com.tribbloids.spookystuff.TestBeans.Example
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyEnvFixture}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.Ignore

/**
  * Created by peng on 09/07/16.
  */
@Ignore // TODO: remove due to deprecation of ScalaDynamicMixin
class ScalaDynamicExtractorSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  import org.apache.spark.ml.dsl.utils.refl.ScalaType._

  val doc: DocOption = Wget(HTML_URL).fetch(spooky).head

  it("can resolve Fetched.timestamp") {

    val result = doc.timestamp

    def dynamic = ScalaDynamicExtractor(
      Lit(doc),
      "timestamp",
      None
    )

    dynamic.resolveType(null) should_=~= TimestampType
    val fn = dynamic.resolve(null)
    assert(fn.apply(null) == result)
  }

  it("can resolve Doc.uri") {

    val result = doc.asInstanceOf[Doc].uri

    def dynamic = ScalaDynamicExtractor(
      Lit(doc.asInstanceOf[Doc]),
      "uri",
      None
    )

    dynamic.resolveType(null) should_=~= StringType
    val fn = dynamic.resolve(null)
    assert(fn.apply(null) == result)
  }

  it("can resolve Unstructured.code") {

    val result = doc.root.asInstanceOf[Unstructured].code.get

    def dynamic = ScalaDynamicExtractor(
      Lit(doc.root.asInstanceOf[Unstructured]),
      "code",
      None
    )

    dynamic.resolveType(null) should_=~= StringType
    val fn = dynamic.resolve(null)
    val dynamicResult = fn.apply(null)
    assert(dynamicResult == result)
  }

  // useless at the moment
  it("can resolve Action.dryrun") {

    val action: Action = Wget(HTML_URL)

    val result = action.dryRun

    def dynamic = ScalaDynamicExtractor(
      Lit[Action](action),
      "dryrun",
      None
    )

    dynamic.resolveType(null) should_=~= ArrayType(ArrayType(new ActionUDT()))
    val fn = dynamic.resolve(null)
    assert(fn.apply(null) == result)
  }

  //  test("can resolve function of String.startsWith(String) using Scala") {
  //    {
  //      def dynamic = ScalaDynamicExtractor(
  //        Literal("abcde"),
  //        "startsWith",
  //        Option(List(Literal("abc")))
  //      )
  //
  //      val impl = dynamic.resolveUsingScala(null)
  //      val result = impl.apply(null).get
  //
  //      assert(result == true)
  //    }
  //    {
  //      def dynamic = ScalaDynamicExtractor(
  //        Literal("abcde"),
  //        "startsWith",
  //        Option(List(Literal("abd")))
  //      )
  //
  //      val impl = dynamic.resolveUsingScala(null)
  //      val result = impl.apply(null).get
  //
  //      assert(result == false)
  //    }
  //  }

  import com.tribbloids.spookystuff.dsl._

  // deliberately stressful to ensure thread safety
  //  val src = sc.parallelize(0 to 1023)
  //    .map {
  //      i =>
  //        (
  //          Some(new Example(Random.nextString(i), Random.nextInt())).filter(_.b >= 0),
  //          Some(Random.nextInt()).filter(_ >= 0),
  //          Random.nextString(i)
  //          )
  //    }

  val src = List(
    (Some(new Example()), Some(2), "abc"),
    (Some(new Example()), Some(1), "abc"),
    (Some(new Example()), None, "abd"),
    (None, Some(2), "abe")
  )
  val df: DataFrame = sql.createDataFrame(src).toDF("A", "B", "C")
  val ds: FetchedDataset = spooky
    .create(df)
    .fetch(
      Wget(HTML_URL)
    )

  val rdd: RDD[FR] = ds.unsquashedRDD.persist()
  val rows: Array[FR] = rdd.take(10)
  override lazy val emptySchema: SpookySchema = ds.schema

  val getNullType: Lit[FR, Null] = Lit[Null](null)

  def verifyOnDriverAndWorkers(dynamic: ScalaDynamicExtractor[FR], staticFn: FR => Option[Any]): Unit = {

    val dynamicFn: (FR) => Option[Any] = dynamic.resolve(emptySchema).lift

    {
      rows.foreach { row =>
        val dd = dynamicFn.apply(row)
        val ss = staticFn.apply(row)
        Predef.assert(dd == ss, s"$dd != $ss")
      }
    }

    {
      rdd.foreach { row =>
        val dd = dynamicFn.apply(row)
        val ss = staticFn.apply(row)
        Predef.assert(dd == ss, s"$dd != $ss")
      }
    }
  }

  it("can resolve a defined class method") {

    def dynamic = ScalaDynamicExtractor(
      'A,
      "fn",
      Some(List[Get]('B))
    )
    val staticFn: (FR) => Option[Any] = { fr =>
      val dr = fr.dataRow
      val result =
        for (
          a <- dr.get('A);
          b <- dr.get('B)
        ) yield {
          a.asInstanceOf[Example].fn(b.asInstanceOf[Int])
        }
      result
    }

    dynamic.resolveType(emptySchema) should_=~= StringType
    verifyOnDriverAndWorkers(dynamic, staticFn)
  }

  it("can resolve a defined class method that has option return type") {

    def dynamic = ScalaDynamicExtractor(
      'A,
      "fnOpt",
      Some(List[Get]('B))
    )
    val staticFn: (FR) => Option[Any] = { fr =>
      val dr = fr.dataRow
      val result =
        for (
          a <- dr.get('A);
          b <- dr.get('B)
        ) yield {
          a.asInstanceOf[Example].fnOpt(b.asInstanceOf[Int])
        }
      result.flatten
    }

    dynamic.resolveType(emptySchema) should_=~= IntegerType
    verifyOnDriverAndWorkers(dynamic, staticFn)
  }

  it("can resolve String.concat(String)") {
    def dynamic = ScalaDynamicExtractor(
      Lit("abcde"),
      "concat",
      Some(List[Get]('C))
    )
    val staticFn: (FR) => Option[Any] = { fr =>
      val dr = fr.dataRow
      val result = for (c <- dr.get('C)) yield {
        "abcde" concat c.asInstanceOf[String]
      }
      result
    }

    dynamic.resolveType(emptySchema) should_=~= StringType
    verifyOnDriverAndWorkers(dynamic, staticFn)
  }

  it("can resolve Array[String].length") {
    def dynamic = ScalaDynamicExtractor(
      Lit("a b c d e".split(" ")),
      "length",
      None
    )

    dynamic.resolveType(emptySchema) should_=~= IntegerType
    val dynamicFn = dynamic.resolve(emptySchema).lift
    assert(dynamicFn.apply(null) == Some(5))
  }

  it("can resolve type of List[String].head") {
    def dynamic = ScalaDynamicExtractor(
      Lit("a b c d e".split(" ").toList),
      "head",
      None
    )

    dynamic.resolveType(null) should_=~= StringType
    val dynamicFn = dynamic.resolve(emptySchema).lift
    assert(dynamicFn.apply(null) == Some("a"))
  }

  it("can resolve type of Seq[String].head") {
    def dynamic = ScalaDynamicExtractor(
      Lit("a b c d e".split(" ").toSeq),
      "head",
      None
    )

    dynamic.resolveType(null) should_=~= StringType
    val dynamicFn = dynamic.resolve(emptySchema).lift
    assert(dynamicFn.apply(null) == Some("a"))
  }

  //  test("can resolve function when base yields NULL") {
  //
  //    def dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fn",
  //      Some(List[GetExpr]('B))
  //    )
  //
  //    val fn = dynamic.resolve(schema)
  //    val result = fn.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function when arg yields NULL") {
  //
  //    def dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fn",
  //      Some(List[GetExpr]('BNull))
  //    )
  //
  //    val fn = dynamic.resolve(schema)
  //    val result = fn.lift.apply(row)
  //    assert(result.isEmpty)
  //  }

  // TODO: this will change in the future
  it("cannot resolve function when base type is NULL") {

    def dynamic = ScalaDynamicExtractor(
      getNullType,
      "fn",
      Some(List[Get]('B))
    )

    intercept[UnsupportedOperationException] {
      dynamic.resolve(emptySchema)
    }
  }

  it("cannot resolve function when arg type is NULL") {

    def dynamic = ScalaDynamicExtractor(
      'A,
      "fn",
      Some(List(getNullType))
    )

    intercept[UnsupportedOperationException] {
      dynamic.resolve(emptySchema)
    }
  }

  //  test("can resolve function that takes monad parameter") {
  //
  //    def dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val fn = dynamic.resolve(schema)
  //    val result = fn.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function that takes monad arg that yields NULL") {
  //
  //    def dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val fn = dynamic.resolve(schema)
  //    val result = fn.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function that takes monad arg of which type is NULL") {
  //
  //    def dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val fn = dynamic.resolve(schema)
  //    val result = fn.lift.apply(row)
  //    assert(result.isEmpty)
  //  }

  it("can resolve function of String.startsWith(String) using Java") {
    {
      def dynamic = ScalaDynamicExtractor(
        Lit("abcde"),
        "startsWith",
        Option(List(Lit("abc")))
      )

      val impl = dynamic.resolveUsingJava(null)
      val result = impl.apply(null).get

      assert(result == true)
    }
    {
      def dynamic = ScalaDynamicExtractor(
        Lit("abcde"),
        "startsWith",
        Option(List(Lit("abd")))
      )

      val impl = dynamic.resolveUsingJava(null)
      val result = impl.apply(null).get

      assert(result == false)
    }
  }

  // TODO: remove or optimize Java implementation
  ignore("Performance test: Java reflection should be faster than ScalaReflection") {
    val int2Str: GenExtractor[Int, String] = { i: Int =>
      "" + i
    }

    val int2_10: GenExtractor[Int, String] = { _: Int =>
      "10"
    }
    def dynamic = ScalaDynamicExtractor[Int](
      int2Str,
      "startsWith",
      Option(List(int2_10))
    )

    val ints = 1 to 1000000

    val pfScala = dynamic.resolveUsingScala(IntegerType)
    val (scalaRes, scalaTime) = CommonUtils.timed(
      ints.map(i => pfScala.apply(i).get.asInstanceOf[Boolean])
    )
    println(scalaTime)

    val pfJava = dynamic.resolveUsingScala(IntegerType)
    val (javaRes, javaTime) = CommonUtils.timed(
      ints.map(i => pfJava.apply(i).get.asInstanceOf[Boolean])
    )
    println(javaTime)

    val (nativeRes, nativeTime) = CommonUtils.timed(
      ints.map(i => int2Str(i).startsWith(int2_10(i)))
    )
    println(nativeTime)

    assert((scalaRes.count(v => v): Int) == (javaRes.count(v => v): Int))
    assert((nativeRes.count(v => v): Int) == (javaRes.count(v => v): Int))
    assert(javaTime < scalaTime)
  }

  //  test("Performance test: Java reflection should be faster than ScalaReflection") {
  //    val int2Str: GenExtractor[Int, String] = { i: Int => "" + i }
  //
  //    val int2_10: GenExtractor[Int, String] = { i: Int => "10" }
  //    def dynamic = ScalaDynamicExtractor[Int](
  //      int2Str,
  //      "startsWith",
  //      Option(List(int2_10))
  //    )
  //
  //    val ints = 1 to 1000000
  //
  //    val pfScala = dynamic.resolveUsingScala(IntegerType)
  //    val (scalaRes, scalaTime) = CommonUtils.timer(
  //      ints.map(
  //        i =>
  //          pfScala.apply(i).asInstanceOf[Boolean]
  //      )
  //    )
  //    println(scalaTime)
  //
  //    val pfJava= dynamic.resolveUsingScala(IntegerType)
  //    val (javaRes, javaTime) = CommonUtils.timer(
  //      ints.map(
  //        i =>
  //          pfJava.apply(i).asInstanceOf[Boolean]
  //      )
  //    )
  //    println(javaTime)
  //
  //    val (nativeRes, nativeTime) = CommonUtils.timer(
  //      ints.map(
  //        i =>
  //          int2Str(i).startsWith(int2_10(i))
  //      )
  //    )
  //    println(nativeTime)
  //
  //    assert((scalaRes.count(v => v): Int) == (javaRes.count(v => v): Int))
  //    assert((nativeRes.count(v => v): Int) == (javaRes.count(v => v): Int))
  //    assert(javaTime < scalaTime)
  //  }
}
