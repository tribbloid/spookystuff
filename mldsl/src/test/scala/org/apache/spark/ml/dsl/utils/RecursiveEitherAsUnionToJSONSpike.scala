package org.apache.spark.ml.dsl.utils

import RecursiveEitherAsUnionToJSONSpike.{Inclusive, Test1, Test2, Union}
import org.apache.spark.ml.dsl.utils.messaging.{MessageReader, MessageWriter}
import org.scalatest.FunSpec
import org.slf4j.LoggerFactory

object RecursiveEitherAsUnionToJSONSpike {

  type ||[A, B] = Either[A, B]

  type Union = String || Test2 || Test1

  case class Test1(
      str: String,
      int: Int
  )

  case class Test2(
      d: Double
  )

  case class Inclusive(v: Union, x: String)
}

class RecursiveEitherAsUnionToJSONSpike extends FunSpec {

  val u1: Union = Right(Test1("abc", 2))
  val u2: Union = Left(Right(Test2(3.2)))
  val u3: Union = Left(Left("def"))

  it("JSON <=> Union of arity 3") {

    Seq(u1, u2, u3).foreach { u =>
      val json = MessageWriter(u).prettyJSON
      LoggerFactory.getLogger(this.getClass).info(json)
      val back = new MessageReader[Union].fromJSON(json)
      assert(back == u)
    }
  }

  it("JSON <=> case class with Union of arity 3") {

    Seq(u1, u2, u3).foreach { i =>
      val u = Inclusive(i, "xyz")
      val json = MessageWriter(u).prettyJSON
      LoggerFactory.getLogger(this.getClass).info(json)
      val back = new MessageReader[Inclusive].fromJSON(json)
      assert(back == u)
    }
  }
}
