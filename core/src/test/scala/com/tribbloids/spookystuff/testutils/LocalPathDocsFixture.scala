package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.utils.SpookyUtils

trait LocalPathDocsFixture extends RemoteDocsFixture {

  //TODO: this should be within TEMP_PATH, however current temp directory cleanup is broken and may results in resources extracted in new suite being deleted by previous suite
  final val TESTUTILS_TEMP_PATH = TestHelper.TARGET_PATH + "resources/testutils"
  final val RESOURCE_NAME = "testutils"

  {
    val resourceOpt = SpookyUtils.getCPResource(RESOURCE_NAME)
    resourceOpt.foreach {
      resource =>
        SpookyUtils.extractResource(
          resource, TESTUTILS_TEMP_PATH
        )
    }
  }

  def unpack(resource: String): String = {
    TESTUTILS_TEMP_PATH + resource.stripPrefix(RESOURCE_NAME)
  }

  val _HTML_URL = unpack("testutils/site/Wikipedia.html")
  val _JSON_URL = unpack("testutils/site/tribbloid.json")
  val _PNG_URL =  unpack("testutils/site/logo11w.png")
  val _PDF_URL = unpack("testutils/site/Test.pdf")
  val _XML_URL = unpack("testutils/site/pom.xml")
  val _CSV_URL = unpack("testutils/site/table.csv")
  val _DIR_URL = unpack("testutils/site")

  override def HTML_URL = _HTML_URL
  override def JSON_URL = _JSON_URL
  override def PNG_URL =  _PNG_URL
  override def PDF_URL = _PDF_URL
  override def XML_URL = _XML_URL
  override def CSV_URL = _CSV_URL

  def DIR_URL = _DIR_URL
}