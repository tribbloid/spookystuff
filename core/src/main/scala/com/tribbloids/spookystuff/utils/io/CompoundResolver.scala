package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}

case class CompoundResolver(
                             factory: String => URIResolver
                           ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) =
    factory(pathStr).input(pathStr)(f)

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) =
    factory(pathStr).output(pathStr, overwrite)(f)

  override def lockAccessDuring[T](pathStr: String)(f: String => T) =
    factory(pathStr).lockAccessDuring(pathStr)(f)
}
