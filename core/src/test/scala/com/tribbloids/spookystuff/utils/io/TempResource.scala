package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

import scala.util.Random

case class TempResource(
    resolver: URIResolver,
    pathStr: String,
    includeSnapshot: Boolean = true
) extends LocalCleanable {

  import TempResource._

  lazy val session = resolver.newSession(pathStr)

  def absolutePathStr: String = session.absolutePathStr

  private object snapshot {

    val self: Snapshot = session.snapshot()

    lazy val dir: URISession = self.TempDir.session
    lazy val old: URIResolver#URISession = session.outer.newSession(self.oldFilePath)
  }

  lazy val sessions: Seq[URIResolver#URISession] = {
    if (includeSnapshot) Seq(session, snapshot.dir, snapshot.old)
    else Seq(session)
  }

  def requireVoid[T](fn: => T): T = {

    delete()
    val result = fn
    delete()
    result
  }

  def requireEmptyFile[T](fn: => T): T = {

    delete()
    session.output(WriteMode.CreateOnly)(out => out.stream)
    val result = fn
    delete()
    result
  }

  def requireRandomFile[T](length: Int = defaultRandomFileSize)(fn: => T): T = {
    delete()
    session.output(WriteMode.CreateOnly) { out =>
      val bytes = Array.ofDim[Byte](length)
      Random.nextBytes(bytes)
      out.stream.write(bytes)
    }
    val result = fn
    delete()
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
