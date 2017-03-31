package com.tribbloids.spookystuff.testutils

import java.io.File
import java.net.URL
import java.nio.file.{Path, Paths}

import com.tribbloids.spookystuff.utils.SpookyUtils

/**
  * Created by peng on 20/09/16.
  */
case class ResourceJARResolver(rootPath: String) {

  //TODO: this should be within TEMP_PATH, however current temp directory cleanup is broken and may results in resources extracted in new suite being deleted by previous suite
  final val UNPACK_RESOURCE_PATH = SpookyUtils.:\(SpookyUtils.\\\(
    TestHelper.UNPACK_RESOURCE_PATH,
    "generated-resources",
    rootPath
  ))
  final val RESOURCE_NAME = rootPath + File.separator

  // run once and for all TODO: or clean up at shutdown hook
  val testResources: Unit = {
    val resourceOpt = SpookyUtils.getCPResource(RESOURCE_NAME)
    resourceOpt.foreach {
      resource =>
        SpookyUtils.extractResource(
          resource, UNPACK_RESOURCE_PATH
        )
    }
    Thread.sleep(5000) //for eventual consistency
  }

  def unpacked(resource: String): String = {

    UNPACK_RESOURCE_PATH + resource.stripPrefix(RESOURCE_NAME)
  }

  def unpackedPath(resource: String): Path = {
    Paths.get(unpacked(resource))
  }

  def unpackedURL(resource: String): URL = {
    new URL("file://" + unpacked(resource)) //TODO: change to File(..).getURL?
  }
}
