package com.tribbloids.spookystuff.extractors

import java.sql.Timestamp

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.{Action, ActionUDT, Wget}
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.tests.TestHelper
import org.apache.spark.sql.types._

/**
  * Created by peng on 09/07/16.
  */

class ScalaDynamicExtractorSuite extends SpookyEnvSuite {

  //  test("can resolve type of List[String].head") {
  //    val dynamic = ScalaDynamicExtractor(
  //      Literal("a b c d e".split(" ").toList),
  //      "head",
  //      List()
  //    )
  //
  //    assert(dynamic.resolveType(null) == StringType)
  //  }
  //
  //  test("can resolve type of Seq[String].head") {
  //    val dynamic = ScalaDynamicExtractor (
  //      Literal("a b c d e".split(" ").toSeq),
  //      "head",
  //      List()
  //    )
  //
  //    assert(dynamic.resolveType(null) == StringType)
  //  }

  import com.tribbloids.spookystuff.dsl._
  import com.tribbloids.spookystuff.utils.ImplicitUtils._
  val doc = Wget(HTML_URL).fetch(spooky).head

  test("can resolve type of Fetched.timestamp") {

    val ts: Timestamp = doc.timestamp

    val dynamic = ScalaDynamicExtractor (
      Literal(doc),
      "timestamp",
      None
    )

    assert(dynamic.resolveType(null) =~= TimestampType)
  }

  test("can resolve type of Doc.uri") {

    val dynamic = ScalaDynamicExtractor (
      Literal(doc.asInstanceOf[Doc]),
      "uri",
      None
    )

    assert(dynamic.resolveType(null) =~= StringType)
  }

  test("can resolve type of Doc.code") {

    val dynamic = ScalaDynamicExtractor (
      Literal(doc.asInstanceOf[Doc]),
      "code",
      None
    )

    assert(dynamic.resolveType(null) =~= StringType)
  }

  //useless at the moment
  test("can resolve type of Action.dryrun") {
    import com.tribbloids.spookystuff.dsl._

    val action: Action = Wget(HTML_URL)

    val ts = action.dryrun

    val dynamic = ScalaDynamicExtractor (
      Literal[Action](action),
      "dryrun",
      None
    )

    assert(dynamic.resolveType(null) =~= ArrayType(ArrayType(new ActionUDT())))
  }

  test("can resolve function of String.startsWith(String) using Scala") {
    {
      val dynamic = ScalaDynamicExtractor(
        Literal("abcde"),
        "startsWith",
        Option(List(Literal("abc")))
      )

      val impl = dynamic.resolveUsingScala(null)
      val result = impl.apply(null).get

      assert(result == true)
    }
    {
      val dynamic = ScalaDynamicExtractor(
        Literal("abcde"),
        "startsWith",
        Option(List(Literal("abd")))
      )

      val impl = dynamic.resolveUsingScala(null)
      val result = impl.apply(null).get

      assert(result == false)
    }
  }

  test("can resolve function of String.startsWith(String) using Java") {
    {
      val dynamic = ScalaDynamicExtractor(
        Literal("abcde"),
        "startsWith",
        Option(List(Literal("abc")))
      )

      val impl = dynamic.resolveUsingJava(null)
      val result = impl.apply(null).get

      assert(result == true)
    }
    {
      val dynamic = ScalaDynamicExtractor(
        Literal("abcde"),
        "startsWith",
        Option(List(Literal("abd")))
      )

      val impl = dynamic.resolveUsingJava(null)
      val result = impl.apply(null).get

      assert(result == false)
    }
  }

  import com.tribbloids.spookystuff.dsl._

  private val tuples: List[(Option[Example], Option[Int], String)] = List(
    (Some(new Example()), Some(2), "abc"),
    (Some(new Example()), Some(1), "abc"),
    (Some(new Example()), None, "abd"),
    (None, Some(2), "abe")
  )
  val df = sql.createDataFrame(tuples).toDF("A", "B", "C")
  val ds = spooky.create(df)
  val rows = ds.unsquashedRDD.collect()
  override lazy val schema = ds.schema

  val getNullType = Literal[Null](null)

  test("can resolve a function") {

    val dynamic = ScalaDynamicExtractor(
      'A,
      "fn",
      Some(List[GetExpr]('B))
    )

    assert(dynamic.resolveType(schema) =~= StringType)
    val resolved = dynamic.resolve(schema)
    assert(resolved.lift.apply(rows(0)) == Some("12"))
    assert(resolved.lift.apply(rows(1)) == Some("11"))
    assert(resolved.lift.apply(rows(2)).isEmpty)
    assert(resolved.lift.apply(rows(3)).isEmpty)
  }

  test("can resolve a function that has monad output") {

    val dynamic = ScalaDynamicExtractor(
      'A,
      "fnOpt",
      Some(List[GetExpr]('B))
    )

    assert(dynamic.resolveType(schema) =~= IntegerType)
    val resolved = dynamic.resolve(schema)
    assert(resolved.lift.apply(rows(0)).isEmpty)
    assert(resolved.lift.apply(rows(1)) == Some(1))
    assert(resolved.lift.apply(rows(2)).isEmpty)
    assert(resolved.lift.apply(rows(3)).isEmpty)
  }

  test("can resolve type of String.startsWith(String)") {
    val dynamic = ScalaDynamicExtractor(
      Literal("abcde"),
      "startsWith",
      Some(List[GetExpr]('C))
    )

    assert(dynamic.resolveType(schema) =~= BooleanType)
    val resolved = dynamic.resolve(schema)
    assert(resolved.lift.apply(rows(0)) == Some(true))
    assert(resolved.lift.apply(rows(1)) == Some(true))
    assert(resolved.lift.apply(rows(2)) == Some(false))
    assert(resolved.lift.apply(rows(3)) == Some(false))
  }

  test("can resolve type of Array[String].length") {
    val dynamic = ScalaDynamicExtractor(
      Literal("a b c d e".split(" ")),
      "length",
      None
    )

    assert(dynamic.resolveType(schema) =~= IntegerType)
    val resolved = dynamic.resolve(schema)
    assert(resolved.lift.apply(rows(0)) == Some(5))
  }

  //  test("can resolve function when base yields NULL") {
  //
  //    val dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fn",
  //      Some(List[GetExpr]('B))
  //    )
  //
  //    val resolved = dynamic.resolve(schema)
  //    val result = resolved.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function when arg yields NULL") {
  //
  //    val dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fn",
  //      Some(List[GetExpr]('BNull))
  //    )
  //
  //    val resolved = dynamic.resolve(schema)
  //    val result = resolved.lift.apply(row)
  //    assert(result.isEmpty)
  //  }

  //TODO: this will change in the future
  test("cannot resolve function when base type is NULL") {

    val dynamic = ScalaDynamicExtractor(
      getNullType,
      "fn",
      Some(List[GetExpr]('B))
    )

    intercept[UnsupportedOperationException] {
      val resolved = dynamic.resolve(schema)
    }
  }

  test("cannot resolve function when arg type is NULL") {

    val dynamic = ScalaDynamicExtractor(
      'A,
      "fn",
      Some(List(getNullType))
    )

    intercept[UnsupportedOperationException] {
      val resolved = dynamic.resolve(schema)
    }
  }

  //  test("can resolve function that takes monad parameter") {
  //
  //    val dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val resolved = dynamic.resolve(schema)
  //    val result = resolved.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function that takes monad arg that yields NULL") {
  //
  //    val dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val resolved = dynamic.resolve(schema)
  //    val result = resolved.lift.apply(row)
  //    assert(result.isEmpty)
  //  }
  //
  //  test("can resolve function that takes monad arg of which type is NULL") {
  //
  //    val dynamic = ScalaDynamicExtractor(
  //      'A,
  //      "fnOpt",
  //      Some(List[GetExpr]('ANull))
  //    )
  //
  //    val resolved = dynamic.resolve(schema)
  //    val result = resolved.lift.apply(row)
  //    assert(result.isEmpty)
  //  }

  test("Performance test: Java reflection should be faster than ScalaReflection") {
    val int2Str: GenExtractor[Int, String] = { i: Int => "" + i }

    val int2_10: GenExtractor[Int, String] = { i: Int => "10" }
    val dynamic = ScalaDynamicExtractor[Int](
      int2Str,
      "startsWith",
      Option(List(int2_10))
    )

    val ints = 1 to 1000000

    val pfScala = dynamic.resolveUsingScala(IntegerType)
    val (scalaRes, scalaTime) = TestHelper.timer(
      ints.map(
        i =>
          pfScala.apply(i).get.asInstanceOf[Boolean]
      )
    )
    println(scalaTime)

    val pfJava= dynamic.resolveUsingScala(IntegerType)
    val (javaRes, javaTime) = TestHelper.timer(
      ints.map(
        i =>
          pfJava.apply(i).get.asInstanceOf[Boolean]
      )
    )
    println(javaTime)

    val (nativeRes, nativeTime) = TestHelper.timer (
      ints.map(
        i =>
          int2Str(i).startsWith(int2_10(i))
      )
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
  //    val dynamic = ScalaDynamicExtractor[Int](
  //      int2Str,
  //      "startsWith",
  //      Option(List(int2_10))
  //    )
  //
  //    val ints = 1 to 1000000
  //
  //    val pfScala = dynamic.resolveUsingScala(IntegerType)
  //    val (scalaRes, scalaTime) = TestHelper.timer(
  //      ints.map(
  //        i =>
  //          pfScala.apply(i).asInstanceOf[Boolean]
  //      )
  //    )
  //    println(scalaTime)
  //
  //    val pfJava= dynamic.resolveUsingScala(IntegerType)
  //    val (javaRes, javaTime) = TestHelper.timer(
  //      ints.map(
  //        i =>
  //          pfJava.apply(i).asInstanceOf[Boolean]
  //      )
  //    )
  //    println(javaTime)
  //
  //    val (nativeRes, nativeTime) = TestHelper.timer(
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
