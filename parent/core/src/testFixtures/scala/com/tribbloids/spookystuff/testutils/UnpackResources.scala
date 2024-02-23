package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.CommonConst
import com.tribbloids.spookystuff.commons.classpath.ClasspathResolver
import com.tribbloids.spookystuff.commons.lifespan.Cleanable
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.io.{LocalResolver, WriteMode}

import java.net.URL
import java.nio.file.{Path, Paths}

/**
  * Created by peng on 20/09/16.
  */
case class UnpackResources(
    root: String,
    unpackedParent: String = CommonConst.UNPACK_RESOURCE_DIR
) extends Cleanable {

  {
    deleteUnpackedParent()
    Thread.sleep(100) // wait for eventual consistency
  }

  // run once and for all TODO: or clean up at shutdown hook
  lazy val unpackOnce: Unit = {
    ClasspathResolver
      .execute(root)
      .treeCopyTo(
        LocalResolver.execute(unpackedParent),
        WriteMode.Overwrite
      )
    Thread.sleep(5000) // for eventual consistency
  }

  def deleteUnpackedParent(): Unit = {
    TestHelper.cleanTempDirs(Seq(unpackedParent))
  }

  def unpacked(resourcePath: String): String = {
    unpackOnce
    CommonUtils.\\\(unpackedParent, resourcePath)
  }

  def unpackedPath(resourcePath: String): Path = {
    Paths.get(unpacked(resourcePath))
  }

  def unpackedURL(resourcePath: String): URL = {
    new URL("file://" + unpacked(resourcePath)) // TODO: change to File(..).getURL?
  }

  override protected def cleanImpl(): Unit = deleteUnpackedParent()
}
