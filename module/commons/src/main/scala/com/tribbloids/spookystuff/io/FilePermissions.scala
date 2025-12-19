package com.tribbloids.spookystuff.io

/**
  * Cross-platform file permission types that can be used across different resolver implementations (LocalResolver,
  * HDFSResolver, etc.)
  */
sealed trait FilePermissionType

object FilePermissionType {
  case object Executable extends FilePermissionType
  case object Writable extends FilePermissionType
  case object Readable extends FilePermissionType
}
