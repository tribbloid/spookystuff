package com.tribbloids.spookystuff.spike

import com.tribbloids.spookystuff.relay.Relay
import com.tribbloids.spookystuff.relay.io.Encoder
import com.tribbloids.spookystuff.spike.RecursiveEitherAsUnionToJSONSpike._
import com.tribbloids.spookystuff.testutils.BaseSpec
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
  case class InclusiveOpt(v: Option[Union], x: String)
}

class RecursiveEitherAsUnionToJSONSpike extends BaseSpec {

  val u1: Union = Right(Test1("abc", 2))
  val u2: Union = Left(Right(Test2(3.2)))
  val u3: Union = Left(Left("def"))

  it("JSON <=> Union of arity 3") {

    Seq(u1, u2, u3).foreach { u =>
      val json = Encoder.forValue(u).prettyJSON
      LoggerFactory.getLogger(this.getClass).info(json)
      val back = new Relay.ToSelf[Union].fromJSON(json)
      assert(back == u)
    }
  }

  it("JSON <=> case class with Union of arity 3") {

    Seq(u1, u2, u3).foreach { u =>
      val inclusie = Inclusive(u, "xyz")
      val json = Encoder.forValue(inclusie).prettyJSON
      LoggerFactory.getLogger(this.getClass).info(json)
      val back = new Relay.ToSelf[Inclusive].fromJSON(json)
      assert(back == inclusie)
    }
  }

  it("JSON <=> case class with Option[Union] of arity 3") {

    val proto = Seq(u1, u2, u3).map { u =>
      InclusiveOpt(Some(u), "xyz")
    } :+ InclusiveOpt(None, "z")

    proto
      .foreach { opt =>
        val json = Encoder.forValue(opt).prettyJSON
        LoggerFactory.getLogger(this.getClass).info(json)
        val back = new Relay.ToSelf[InclusiveOpt].fromJSON(json)
        assert(back == opt)
      }

    InclusiveOpt(None, "xyz")
  }
}
