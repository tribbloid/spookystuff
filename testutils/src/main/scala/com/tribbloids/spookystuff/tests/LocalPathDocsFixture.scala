package com.tribbloids.spookystuff.tests

trait LocalPathDocsFixture extends RemoteDocsFixture {

  def unpack(resource: String): String = {
    TestHelper.unpackResourceIfNotExist(resource)
  }

  val _HTML_URL = unpack("site/Wikipedia.html")
  val _JSON_URL = unpack("site/tribbloid.json")
  val _PNG_URL =  unpack("site/logo11w.png")
  val _PDF_URL = unpack("site/Test.pdf")
  val _XML_URL = unpack("site/pom.xml")
  val _CSV_URL = unpack("site/table.csv")
  val _DIR_URL = unpack("site")

  override def HTML_URL = _HTML_URL
  override def JSON_URL = _JSON_URL
  override def PNG_URL =  _PNG_URL
  override def PDF_URL = _PDF_URL
  override def XML_URL = _XML_URL
  override def CSV_URL = _CSV_URL

  def DIR_URL = _DIR_URL
}