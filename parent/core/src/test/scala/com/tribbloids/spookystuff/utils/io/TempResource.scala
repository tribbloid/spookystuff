package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

import scala.util.Random

case class TempResource(
    resolver: URIResolver,
    pathStr: String
) extends LocalCleanable {

  import TempResource._

  lazy val execution: resolver.Execution = resolver.execute(pathStr)
  def absolutePathStr: String = execution.absolutePathStr

//  def absolutePathStr: String = session.absolutePathStr

  lazy val executions: Seq[URIResolver#Execution] = {
    Seq(execution)
  }

  def deleteBeforeAndAfter[T](fn: => T): T = {
    delete()
    try {
      fn
    } finally {
      delete()
    }
  }

  def requireVoid[T](fn: => T): T = deleteBeforeAndAfter {

    val result = fn
    result
  }

  def requireEmptyFile[T](fn: => T): T = deleteBeforeAndAfter {

    execution.createNew()
    val result = fn
    result
  }

  def requireEmptyDir[T](fn: => T): T = deleteBeforeAndAfter {

    val subExe = resolver.execute(CommonUtils.\\\(execution.absolutePathStr, "Random" + Random.nextLong()))

    subExe.createNew()
    subExe.delete(false)
    val result = fn
    result
  }

  def requireRandomContent[T](length: Int = defaultRandomFileSize)(fn: => T): T = deleteBeforeAndAfter {
    execution.output(WriteMode.CreateOnly) { out =>
      val bytes = Array.ofDim[Byte](length)
      Random.nextBytes(bytes)
      out.stream.write(bytes)
    }
    val result = fn
    result
  }

  def delete(): Unit = {
    executions.foreach { ss =>
      ss.delete(false)
    }
    Thread.sleep(1000)
  }

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = {
    delete()
  }

  def \(subDir: String): TempResource = this.copy(
    pathStr = CommonUtils.\\\(this.pathStr, subDir)
  )
}

object TempResource {

  val defaultRandomFileSize: Int = 16
}
