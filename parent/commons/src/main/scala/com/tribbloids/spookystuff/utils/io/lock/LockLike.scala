package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.io.lock.Lock.InMemoryLock
import com.tribbloids.spookystuff.utils.io.{URIExecution, URIResolver}
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

import java.util.UUID

trait LockLike extends NOTSerializable {

  import LockLike._

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

    lazy val locked: String = CommonUtils.\\\(dir, id + LOCKED)

    lazy val old: String = CommonUtils.\\\(dir, id + OLD)
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
