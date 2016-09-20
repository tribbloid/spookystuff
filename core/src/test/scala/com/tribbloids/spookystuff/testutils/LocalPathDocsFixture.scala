package com.tribbloids.spookystuff.testutils

import java.net.URL
import java.nio.file.{Path, Paths}

import com.tribbloids.spookystuff.utils.SpookyUtils

object LocalPathDocsFixture {

  //TODO: this should be within TEMP_PATH, however current temp directory cleanup is broken and may results in resources extracted in new suite being deleted by previous suite
  final val TESTUTILS_TEMP_PATH = TestHelper.TARGET_PATH + "generated-resources/testutils/"
  final val RESOURCE_NAME = "testutils/"

  // run once and for all TODO: or clean up at shutdown hook
  lazy val testResources: Unit = {
    val resourceOpt = SpookyUtils.getCPResource(RESOURCE_NAME)
    resourceOpt.foreach {
      resource =>
        SpookyUtils.extractResource(
          resource, TESTUTILS_TEMP_PATH
        )
    }
  }

  def unpacked(resource: String): String = {
    testResources

    LocalPathDocsFixture.TESTUTILS_TEMP_PATH + resource.stripPrefix(LocalPathDocsFixture.RESOURCE_NAME)
  }

  def unpackedPath(resource: String): Path = {
    Paths.get(unpacked(resource))
  }

  def unpackedURL(resource: String): URL = {
    new URL("file://" + unpacked(resource)) //TODO: change to File(..).getURL?
  }
}

trait LocalPathDocsFixture extends RemoteDocsFixture {

  import LocalPathDocsFixture._

  override def HTML_URL = unpacked("testutils/files/Wikipedia.html")
  override def JSON_URL = unpacked("testutils/files/tribbloid.json")
  override def PNG_URL =  unpacked("testutils/files/logo11w.png")
  override def PDF_URL = unpacked("testutils/files/Test.pdf")
  override def XML_URL = unpacked("testutils/files/pom.xml")
  override def CSV_URL = unpacked("testutils/files/table.csv")

  def DIR_URL = unpacked("testutils/files")
}