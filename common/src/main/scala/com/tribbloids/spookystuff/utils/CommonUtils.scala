package com.tribbloids.spookystuff.utils

import java.io.File

object CommonUtils {

  def qualifiedName(separator: String)(parts: String*) = {
    parts.flatMap(v => Option(v)).reduceLeftOption(addSuffix(separator, _) + _).orNull
  }
  def addSuffix(suffix: String, part: String) = {
    if (part.endsWith(suffix)) part
    else part+suffix
  }

  def /:/(parts: String*): String = qualifiedName("/")(parts: _*)
  def :/(part: String): String = addSuffix("/", part)

  def \\\(parts: String*): String = qualifiedName(File.separator)(parts: _*)
  def :\(part: String): String = addSuffix(File.separator, part)
}
