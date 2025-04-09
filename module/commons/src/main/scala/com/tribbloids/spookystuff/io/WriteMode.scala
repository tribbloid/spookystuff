package com.tribbloids.spookystuff.io

object WriteMode extends Enumeration {

  sealed abstract class Effective extends WriteMode

  object Append extends Effective // cannot be used for binary file

  sealed abstract class Effective_Binary extends Effective

  object ErrorIfExists extends Effective_Binary
  object Overwrite extends Effective_Binary

  object Disabled_ReadOnly extends WriteMode // TODO: do we need this?

  // TODO: add back with some high-level implementation using apache-io NullOutputStream
  object Ignore extends WriteMode
}

sealed abstract class WriteMode
