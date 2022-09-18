package com.tribbloids.spookystuff.testutils

import java.io.File
import java.net.URL
import java.nio.file.{Path, Paths}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import com.tribbloids.spookystuff.utils.{ClasspathDebugger, CommonConst, CommonUtils, SpookyUtils}

/**
  * Created by peng on 20/09/16.
  */
case class ResourceJARResolver(
    root: String,
    unpackedParent: String = CommonConst.UNPACK_RESOURCE_DIR
) extends Cleanable {

  //TODO: this should be within TEMP_PATH, however current temp directory cleanup is broken and may results in resources extracted in new suite being deleted by previous suite
  final val unpackedRoot = CommonUtils.:\(
    CommonUtils.\\\(
      unpackedParent,
      root
    ))
  final val RESOURCE_NAME = root + File.separator

  {
    deleteUnpackedRoot()
    Thread.sleep(100) // wait for eventual consistency
  }

  // run once and for all TODO: or clean up at shutdown hook
  lazy val unpackOnce: Unit = {
    val resourceOpt = ClasspathDebugger.getResource(RESOURCE_NAME)
    resourceOpt.foreach { resource =>
      SpookyUtils.extractResource(
        resource,
        unpackedRoot
      )
    }
    Thread.sleep(5000) //for eventual consistency
  }

  def deleteUnpackedRoot(): Unit = {
    TestHelper.cleanTempDirs(Seq(unpackedRoot))
  }

  def unpacked(resource: String): String = {
    unpackOnce
    unpackedRoot + resource.stripPrefix(RESOURCE_NAME)
  }

  def unpackedPath(resource: String): Path = {
    Paths.get(unpacked(resource))
  }

  def unpackedURL(resource: String): URL = {
    new URL("file://" + unpacked(resource)) //TODO: change to File(..).getURL?
  }

  override protected def cleanImpl(): Unit = deleteUnpackedRoot()
}
