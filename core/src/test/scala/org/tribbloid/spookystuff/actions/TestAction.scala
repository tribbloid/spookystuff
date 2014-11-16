package org.tribbloid.spookystuff.actions

import org.scalatest.FunSuite
import org.tribbloid.spookystuff.entity.{Key, PageRow}
import org.tribbloid.spookystuff.utils.Utils

import scala.util.Random

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (Utils.interpolateFromMap(null, Map[String,String]()) === None)}

  test("formatEmptyString") {assert (Utils.interpolateFromMap("", Map[String,String]()) === Some(""))}

  test("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Random.nextInt().seconds
    val action = Visit("#{~}").in(randomTimeout)

    val rewritten = action.interpolate(new PageRow(cells = Map(Key("~") -> "http://www.dummy.com")))

    assert(rewritten.get.timeout(null) === randomTimeout)
  }

  test("interpolate should not change name") {

    val action = Wget("#{~}").as('dummy_name)

    val rewritten = action.interpolate(new PageRow(cells = Map(Key("~") -> "http://www.dummy.com")))

    assert(rewritten.get.name === "dummy_name")
  }
}
