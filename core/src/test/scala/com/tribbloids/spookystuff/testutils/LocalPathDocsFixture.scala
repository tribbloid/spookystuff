package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.utils.SpookyUtils

object LocalPathDocsFixture {

  //TODO: this should be within TEMP_PATH, however current temp directory cleanup is broken and may results in resources extracted in new suite being deleted by previous suite
  final val TESTUTILS_TEMP_PATH = TestHelper.TARGET_PATH + "resources/testutils"
  final val RESOURCE_NAME = "testutils"

  // run once per test TODO: or clean up at shutdown hook
  lazy val testResource = {
    val resourceOpt = SpookyUtils.getCPResource(RESOURCE_NAME)
    resourceOpt.foreach {
      resource =>
        SpookyUtils.extractResource(
          resource, TESTUTILS_TEMP_PATH
        )
    }
  }
}

trait LocalPathDocsFixture extends RemoteDocsFixture {

  LocalPathDocsFixture.testResource

  def unpacked(resource: String): String = {
    LocalPathDocsFixture.TESTUTILS_TEMP_PATH + resource.stripPrefix(LocalPathDocsFixture.RESOURCE_NAME)
  }

  val _HTML_URL = unpacked("testutils/site/Wikipedia.html")
  val _JSON_URL = unpacked("testutils/site/tribbloid.json")
  val _PNG_URL =  unpacked("testutils/site/logo11w.png")
  val _PDF_URL = unpacked("testutils/site/Test.pdf")
  val _XML_URL = unpacked("testutils/site/pom.xml")
  val _CSV_URL = unpacked("testutils/site/table.csv")
  val _DIR_URL = unpacked("testutils/site")

  override def HTML_URL = _HTML_URL
  override def JSON_URL = _JSON_URL
  override def PNG_URL =  _PNG_URL
  override def PDF_URL = _PDF_URL
  override def XML_URL = _XML_URL
  override def CSV_URL = _CSV_URL

  def DIR_URL = _DIR_URL
}