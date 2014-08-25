package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import java.util

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (ClientAction.formatWithContext(null, new util.LinkedHashMap[String,String]()) === null)}

  test("formatEmptyString") {assert (ClientAction.formatWithContext("", new util.LinkedHashMap[String,String]()) === "")}
}
