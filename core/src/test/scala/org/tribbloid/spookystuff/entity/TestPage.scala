package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite

/**
 * Created by peng on 22/06/14.
 */
class TestPage  extends FunSuite {

  test("getFileName") {
    val page = new Page(
      "http://dummy",
      "content dummy".getBytes("UTF8"),
      "text/html; charset=UTF-8"
    )

    assert(page.getFilePath("dummy.txt","file:///home/peng") === "file:/home/peng/dummy.txt")
    assert(page.getFilePath("dummy.txt","/home/peng") === "/home/peng/dummy.txt")
    assert(page.getFilePath("dummy.txt","home/peng/") === "home/peng/dummy.txt")
    assert(page.getFilePath("dummy.txt","hdfs://home/peng") === "hdfs://home/peng/dummy.txt")
    assert(page.getFilePath("dummy.txt","s3://home/peng") === "s3://home/peng/dummy.txt")
  }
}
