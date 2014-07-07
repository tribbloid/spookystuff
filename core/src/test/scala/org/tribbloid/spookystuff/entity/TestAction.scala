package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import java.util

/**
 * Created by peng on 07/07/14.
 */
class TestAction extends FunSuite {

  test("formatNullString") {assert (ActionUtils.formatWithContext(null, new util.HashMap[String,String]()) === null)}

  test("formatEmptyString") {assert (ActionUtils.formatWithContext("", new util.HashMap[String,String]()) === "")}
}
