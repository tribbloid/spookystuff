package com.tribbloids.spookystuff.conf

object PluginRegistry extends SystemRegistry {

  type _Sys = PluginSystem

  {
    Core.enableOnce
    Dir.enableOnce
    Python.enableOnce
  }
}
