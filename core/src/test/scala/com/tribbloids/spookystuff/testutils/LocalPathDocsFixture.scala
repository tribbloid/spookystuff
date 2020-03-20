package com.tribbloids.spookystuff.testutils

object TestJARResolver extends ResourceJARResolver("testutils")

trait LocalPathDocsFixture extends RemoteDocsFixture {

  import TestJARResolver._

  override def HTML_URL: String = unpacked("testutils/files/Wikipedia.html")
  override def JSON_URL: String = unpacked("testutils/files/tribbloid.json")
  override def PNG_URL: String = unpacked("testutils/files/logo11w.png")
  override def PDF_URL: String = unpacked("testutils/files/Test.pdf")
  override def XML_URL: String = unpacked("testutils/files/example.xml")
  override def CSV_URL: String = unpacked("testutils/files/table.csv")

  def DIR_URL: String = unpacked("testutils/files")
  def DEEP_DIR_URL: String = unpacked("testutils/dir")
}
