package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.{Key, PageRow}
import com.tribbloids.spookystuff.expressions.Literal
import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.session.DriverSession
import com.tribbloids.spookystuff.{Const, SpookyEnvSuite}

import scala.collection.immutable.ListMap
import scala.util.Random

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends SpookyEnvSuite {

  test("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Visit(Const.keyDelimiter+"{~}").in(randomTimeout)

    val rewritten = action.interpolate(new PageRow(dataRow = ListMap(Key("~") -> "http://www.dummy.com")), spooky).get

    assert(rewritten === Visit(Literal("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
  }

  test("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(new PageRow(dataRow = ListMap(Key("~") -> "http://www.dummy.com")), spooky).get

    assert(rewritten === Wget(Literal("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
  }
}