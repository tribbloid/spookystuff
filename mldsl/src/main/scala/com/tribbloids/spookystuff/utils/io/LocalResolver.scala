package com.tribbloids.spookystuff.utils.io

import java.io._
import java.nio.file.FileAlreadyExistsException

import org.apache.spark.ml.dsl.utils.metadata.MetadataMap

import scala.collection.immutable.ListMap

object LocalResolver extends URIResolver {

  val lockedSuffix: String = ".locked"

  def ensureAbsolute(file: File) = {
    assert(file.isAbsolute, s"BAD DESIGN: ${file.getPath} is not an absolute path")
  }

  override def input[T](pathStr: String)(f: (InputStream) => T) = {

    //    val file = new File(pathStr)
    //    ensureAbsolute(file)

    val file = new File(pathStr)
    new Resource[T] {

      override lazy val value: T = {
        if (!pathStr.endsWith(lockedSuffix)) {
          //wait for its locked file to finish its locked session

          val lockedPath = pathStr + lockedSuffix
          val lockedFile = new File(lockedPath)

          //wait for 15 seconds in total
          retry {
            assert(!lockedFile.exists(), s"File $pathStr is locked by another executor or thread")
          }
        }

        val fis = new FileInputStream(file)

        try {
          f(fis)
        }
        finally {
          fis.close()
        }
      }

      override lazy val metadata = describe(file)
    }

  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T) = {

    val file = new File(pathStr)
    //    ensureAbsolute(file)

    if (file.exists() && !overwrite) throw new FileAlreadyExistsException(s"$pathStr already exists")
    else if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    else if (file.isDirectory) {
      file.delete()

      file.getParentFile.mkdirs()
      file.createNewFile()
    }

    new Resource[T] {

      override val value: T = {

        val fos = new FileOutputStream(pathStr, false)
        try {
          val result = f(fos)
          fos.flush()
          result
        }
        finally {
          fos.close()
        }
      }

      override lazy val metadata = describe(file)
    }
  }

  override def lockAccessDuring[T](pathStr: String)(f: (String) => T): T = {

    val file = new File(pathStr)
    //    ensureAbsolute(file)

    val lockedPath = pathStr + lockedSuffix
    val lockedFile = new File(lockedPath)

    retry {
      assert(!lockedFile.exists(), s"File $pathStr is locked by another executor or thread")
      //        Thread.sleep(3*1000)
    }

    if (file.exists()) file.renameTo(lockedFile)

    try {
      val result = f(lockedPath)
      result
    }
    finally {
      lockedFile.renameTo(file)
    }
  }

  override def toAbsolute(pathStr: String): String = {
    val file = new File(pathStr)
    file.getAbsolutePath
  }

  def describe(file: File): ResourceMD = {

    import Resource._

    def getMap(file: File): ListMap[String, Any] = {
      var map = reflectiveMetadata(file)
      map ++= MetadataMap(
        URI_ -> toAbsolute(file.getAbsolutePath),
        NAME -> file.getName
      )
      if (file.isDirectory)
        map ++= MetadataMap(CONTENT_TYPE -> URIResolver.DIR_TYPE)

      map
    }

    val root = {
      getMap(file)
    }

    if (file.isDirectory) {

      val children = file.listFiles()

      val groupedChildren: Map[String, Seq[Map[String, Any]]] = {

        val withType = children.map {
          child =>
            val tpe = if (child.isDirectory) "directory"
            else if (child.isFile) "file"
            else "others"

            tpe -> child
        }

        withType.groupBy(_._1).mapValues {
          array =>
            array.toSeq.map(kv => getMap(kv._2))
        }
      }
      val result = root ++ groupedChildren
      ResourceMD(result)
    }
    else {
      root
    }
  }
}
