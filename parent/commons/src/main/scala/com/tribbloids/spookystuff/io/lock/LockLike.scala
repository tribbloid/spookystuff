package com.tribbloids.spookystuff.io.lock

import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.io.lock.Lock.InMemoryLock
import com.tribbloids.spookystuff.io.{URIExecution, URIResolver}

import java.util.UUID

trait LockLike extends NOTSerializable {

  import LockLike.*

  lazy val id: UUID = UUID.randomUUID()

  def exe: URIExecution

  val resolver: URIResolver = exe.outer
  def absolutePathStr: String = exe.absolutePathStr

  @transient lazy val inMemory: InMemoryLock = {
    val result = Lock.inMemoryLocks.getOrElseUpdate(exe.outer.getClass -> exe.absolutePathStr, InMemoryLock())
    result
  }

  case object PathStrs {

    lazy val dir: String = exe.absolutePathStr + LOCK

    lazy val locked: String = CommonUtils.\\\(dir, id.toString + LOCKED)

    lazy val old: String = CommonUtils.\\\(dir, id.toString + OLD)
  }

  case object Moved {

    lazy val dir: resolver._Execution = resolver.execute(PathStrs.dir)

    lazy val locked: resolver._Execution = resolver.execute(PathStrs.locked)

    //    lazy val old: resolver.Execution = resolver.Execution(PathStrs.old)
  }
}

object LockLike {

  final val LOCK: String = ".lock"

  final val LOCKED: String = ".locked"

  final val OLD: String = ".old"
}
