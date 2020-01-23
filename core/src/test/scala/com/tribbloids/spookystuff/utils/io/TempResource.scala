package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

import scala.util.Random

case class TempResource(
    resolver: URIResolver,
    pathStr: String,
    includeSnapshot: Boolean = true
) extends LocalCleanable {

  import TempResource._

  lazy val session: resolver.URISession = resolver.newSession(pathStr)

//  def absolutePathStr: String = session.absolutePathStr

  private object snapshot {

    val self: Snapshot = Snapshot(session)

    lazy val dir: URISession = self.tempDir.session
    lazy val old: URIResolver#URISession = session.outer.newSession(self.oldFilePath)
  }

  lazy val sessions: Seq[URIResolver#URISession] = {
    if (includeSnapshot) Seq(session, snapshot.dir, snapshot.old)
    else Seq(session)
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

    session.createNew()
    val result = fn
    result
  }

  def requireRandomFile[T](length: Int = defaultRandomFileSize)(fn: => T): T = deleteBeforeAndAfter {
    session.output(WriteMode.CreateOnly) { out =>
      val bytes = Array.ofDim[Byte](length)
      Random.nextBytes(bytes)
      out.stream.write(bytes)
    }
    val result = fn
    result
  }

  def delete(): Unit = {
    sessions.foreach { ss =>
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
}

object TempResource {

  val defaultRandomFileSize = 16
}
