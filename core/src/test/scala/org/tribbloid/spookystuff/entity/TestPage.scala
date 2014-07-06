package org.tribbloid.spookystuff.entity

import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by peng on 22/06/14.
 */
class TestPage extends FunSuite with BeforeAndAfter {

  var page: Page = null

  before {
    page = new Page(
      "http://dummy",
      "content dummy".getBytes("UTF8"),
      "text/html; charset=UTF-8"
    )
  }

  test("filePath") {assert(page.getFilePath("dummy.txt","file:///home/peng") === "file:/home/peng/dummy.txt")}

  test("rawFilePath") {assert(page.getFilePath("dummy.txt","/home/peng") === "/home/peng/dummy.txt")}

  test("relativeFilePath") {assert(page.getFilePath("dummy.txt","home/peng/") === "home/peng/dummy.txt")}

  test("hdfsFilePath") {assert(page.getFilePath("dummy.txt","hdfs://home/peng") === "hdfs://home/peng/dummy.txt")}

  test("hdfsBadNamedFilePath") {assert(page.getFilePath("dummy_txt","hdfs://peng") === "hdfs://peng/dummy.txt")}

  test("s3FilePath") {assert(page.getFilePath("dummy.txt","s3://home") === "s3://home/dummy.txt")}

  test("s3nFilePath") {assert(page.getFilePath("dummy.txt","s3n://home") === "s3n://home/dummy.txt")}

  test("s3nBadNamedFilePath") {assert(page.getFilePath("Africa_Rice_Center","s3n://home") === "s3n://home/Africa.Rice.Center")}
}

class TestEmptyPage extends FunSuite with BeforeAndAfter {

  var page: Page = null

  before {
    page = PageBuilder.emptyPage
  }

  test("attr1") {assert (page.attr1("div.dummy","href") === null)}

  test("attr") {assert (page.attr("div.dummy","href") === Seq[String]())}

  test("text1") {assert (page.text1("div.dummy") === null)}

  test("text") {assert (page.text("div.dummy") === Seq[String]())}

  test("slice") {assert (page.slice("div.dummy") === Seq[Page]())}

  test("elementExist") {assert (page.elementExist("div.dummy") === false)}

  test("attrExist") {assert (page.attrExist("div.dummy","href") === false)}
}