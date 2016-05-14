package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.expressions.Literal
import com.tribbloids.spookystuff.row.{DataRow, Field}
import com.tribbloids.spookystuff.{Const, SpookyEnvSuite}

import scala.collection.immutable.ListMap
import scala.util.Random

class TestAction extends SpookyEnvSuite {

  test("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Visit(Const.keyDelimiter+"{~}").in(randomTimeout)

    val rewritten = action.interpolate(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")) -> Seq(), spooky).get

    assert(rewritten === Visit(Literal("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
  }

  test("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")) -> Seq(), spooky).get

    assert(rewritten === Wget(Literal("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
  }
}