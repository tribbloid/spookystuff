package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.entity.{Key, PageRow}
import org.tribbloid.spookystuff.expressions.Literal
import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.session.DriverSession
import org.tribbloid.spookystuff.{Const, SpookyEnvSuite}

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

    val rewritten = action.interpolate(new PageRow(cells = ListMap(Key("~") -> "http://www.dummy.com"))).get

    assert(rewritten === Visit(Literal("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
  }

  test("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action.interpolate(new PageRow(cells = ListMap(Key("~") -> "http://www.dummy.com"))).get

    assert(rewritten === Wget(Literal("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
  }
}