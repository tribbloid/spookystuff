package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import org.tribbloid.spookystuff.entity.clientaction.ClientAction

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (ClientAction.interpolate(null, Map[String,String]()) === null)}

  test("formatEmptyString") {assert (ClientAction.interpolate("", Map[String,String]()) === "")}
}
