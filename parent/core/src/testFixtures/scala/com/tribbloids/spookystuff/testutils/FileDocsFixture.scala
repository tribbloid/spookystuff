package com.tribbloids.spookystuff.testutils

trait FileDocsFixture extends RemoteDocsFixture {

  import TestDocsResolver._

  override def HTML_URL: String = unpacked("testutils/dir/Wikipedia.html")
  override def JSON_URL: String = unpacked("testutils/dir/tribbloid.json")
  override def PNG_URL: String = unpacked("testutils/dir/logo11w.png")
  override def PDF_URL: String = unpacked("testutils/dir/Test.pdf")
  override def XML_URL: String = unpacked("testutils/dir/example.xml")
  override def CSV_URL: String = unpacked("testutils/dir/table.csv")

  def DIR_URL: String = unpacked("testutils/dir/dir/dir/dir")
  def DEEP_DIR_URL: String = unpacked("testutils/dir")
}

object FileDocsFixture extends FileDocsFixture
