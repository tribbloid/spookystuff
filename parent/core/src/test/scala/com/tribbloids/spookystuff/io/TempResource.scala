package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.commons.lifespan.LocalCleanable

import scala.util.Random

case class TempResource(
    resolver: URIResolver,
    path: PathMagnet.URIPath
) extends LocalCleanable {

  import TempResource.*

  lazy val execution: resolver.Execution = resolver.on(path)
  def absolutePath = execution.absolutePath

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

    val subExe = resolver.on(
      execution.absolutePath :/ "Random" :/ Random.nextLong().toString
    )

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
    path = this.path :/ subDir
  )
}

object TempResource {

  val defaultRandomFileSize: Int = 16
}
