package com.tribbloids.spookystuff.io.lock

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.io.lock.Lock.InMemoryLock
import com.tribbloids.spookystuff.io.{URIExecution, URIResolver}

import java.util.UUID

trait LockLike extends NOTSerializable {

  import LockLike.*

  lazy val id: UUID = UUID.randomUUID()

  def exe: URIExecution

  val resolver: URIResolver = exe.outer

  @transient lazy val inMemory: InMemoryLock = {
    val result = Lock.inMemoryLocks.getOrElseUpdate(exe.outer.getClass -> exe.absolutePath, InMemoryLock())
    result
  }

  case object PathStrs {

    lazy val lockPath: PathMagnet.URIPath = exe.absolutePath + _LOCK

    lazy val locked: String = lockPath :/ (id.toString + _LOCKED)

    lazy val old: String = lockPath :/ (id.toString + _OLD)
  }

  case object Moved {

    lazy val dir: resolver._Execution = resolver.on(PathStrs.lockPath)

    lazy val locked: resolver._Execution = resolver.on(PathStrs.locked)

    //    lazy val old: resolver.Execution = resolver.Execution(PathStrs.old)
  }
}

object LockLike {

  final val _LOCK: String = ".lock"

  final val _LOCKED: String = ".locked"

  final val _OLD: String = ".old"
}
