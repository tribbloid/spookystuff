package com.tribbloids.spookystuff.io

object WriteMode extends Enumeration {

  sealed abstract class Effective extends WriteMode

  object CreateOnly extends Effective
  object Overwrite extends Effective
  object Append extends Effective

  object ReadOnly extends WriteMode

  // TODO: add back with some high-level implementation using apache-io NullOutputStream
  object Ignore extends WriteMode
}

abstract class WriteMode
