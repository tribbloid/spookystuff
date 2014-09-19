package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import org.tribbloid.spookystuff.entity.client.Action

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (Action.interpolateFromMap(null, Map[String,String]()) === null)}

  test("formatEmptyString") {assert (Action.interpolateFromMap("", Map[String,String]()) === "")}
}
