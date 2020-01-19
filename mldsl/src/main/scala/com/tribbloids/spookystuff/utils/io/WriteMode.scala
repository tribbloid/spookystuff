package com.tribbloids.spookystuff.utils.io

object WriteMode extends Enumeration {

  abstract class Effective extends WriteMode

  object CreateOnly extends Effective
  object Overwrite extends Effective
  object Append extends Effective

  //TODO: add back with some high-level implementation using apache-io NullOutputStream
//  object Ignore extends WriteMode
}

abstract class WriteMode
