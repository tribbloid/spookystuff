package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (Utils.interpolateFromMap(null, Map[String,String]()) === null)}

  test("formatEmptyString") {assert (Utils.interpolateFromMap("", Map[String,String]()) === "")}
}
