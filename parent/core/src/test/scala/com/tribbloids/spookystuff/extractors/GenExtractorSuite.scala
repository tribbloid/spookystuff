package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.testutils.SpookyEnvFixture
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable

import scala.runtime.AbstractPartialFunction

object GenExtractorSuite {

  var counter: Int = 0

  val partialFn: scala.PartialFunction[String, Int] =
    new AbstractPartialFunction[String, Int] with Serializable {

      override def isDefinedAt(v: String): Boolean = {
        counter += 1
        if (v == "abc") true
        else false
      }

      override def applyOrElse[A1 <: String, B1 >: Int](v: A1, default: A1 => B1): B1 = {
        counter += 1
        if (v == "abc") {
          v.length
        } else {
          default(v)
        }
      }
    }

  val optionFn: String => Option[Int] = { v: String =>
    {
      counter += 1
      if (v == "abc") {
        Some(v.length)
      } else {
        None
      }
    }
  }
}

class GenExtractorSuite extends SpookyEnvFixture {

  import GenExtractorSuite._
  import com.tribbloids.spookystuff.dsl._

  it("Some(partialFn) is serializable") {
    AssertSerializable[Option[String => Int]](Some(partialFn), condition = (_, _) => {})
  }

  it("Some(optionFn) is serializable") {
    AssertSerializable[Option[String => Option[Int]]](Some(optionFn), condition = (_, _) => {})
  }

  val tuples: Seq[(GenExtractor[String, Int], String)] = Seq(
    GenExtractor.fromFn(partialFn) -> "fromFn",
    GenExtractor.fromOptionFn(optionFn) -> "fromOptionFn",
    GenExtractor.fromFn(partialFn) ~ 'A -> "Alias(fromFn)",
    GenExtractor.fromOptionFn(optionFn) ~ 'B -> "Alias(fromOptionFn)"
  )

  tuples.foreach { tuple =>
    val extractor = tuple._1
    val str = tuple._2
    it(s"$str and all its resolved functions are serializable") {
      AssertSerializable[GenExtractor[String, Int]](extractor, condition = (_, _) => {})
      AssertSerializable[PartialFunction[String, Int]](extractor.resolve(emptySchema), condition = (_, _) => {})
      AssertSerializable[Function1[String, Option[Int]]](extractor.resolve(emptySchema).lift, condition = (_, _) => {})
    }

    it(s"$str.apply won't execute twice") {
      counter = 0

      println(extractor.apply("abc"))
      assert(counter == 1)

      intercept[MatchError] {
        extractor.apply("d")
      }
      assert(counter == 2)
    }

    it(s"$str.lift.apply won't execute twice") {
      counter = 0

      println(extractor.lift.apply("abc"))
      assert(counter == 1)

      println(extractor.lift.apply("d"))
      assert(counter == 2)
    }

    it(s"$str.applyOrElse won't execute twice") {
      counter = 0

      println(extractor.applyOrElse("abc", (_: String) => 0))
      assert(counter == 1)

      println(extractor.applyOrElse("d", (_: String) => 0))
      assert(counter == 2)
    }

    it(s"$str.isDefined won't execute twice") {
      counter = 0

      println(extractor.isDefinedAt("abc"))
      assert(counter == 1)

      println(extractor.isDefinedAt("d"))
      assert(counter == 2)
    }
  }
}
