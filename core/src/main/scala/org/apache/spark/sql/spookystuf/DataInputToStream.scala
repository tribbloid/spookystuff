package org.apache.spark.sql.spookystuf

import java.io.{Closeable, DataInput, EOFException, InputStream}

/**
  * Wraps [[DataInput]] into [[InputStream]]
  * see https://github.com/jankotek/mapdb/issues/971
  */
case class DataInputToStream(in: DataInput) extends InputStream {

  override def read(b: Array[Byte], off: Int, len: Int): Int = {

    try {
      in.readFully(b, off, len)
      len
    } catch {

      // inefficient way
      case _: ArrayIndexOutOfBoundsException =>
        (off until (off + len)).foreach { i =>
          try {

            val next = in.readByte()
            b.update(i, next)
          } catch {
            case _: EOFException | _: ArrayIndexOutOfBoundsException =>
              return i
          }
        }
        len
    }
  }

  override def skip(n: Long): Long = {
    val _n = Math.min(n, Integer.MAX_VALUE)
    //$DELAY$
    in.skipBytes(_n.toInt)
  }

  override def close(): Unit = {
    in match {
      case closeable: Closeable => closeable.close()
      case _                    =>
    }
  }

  override def read: Int = in.readUnsignedByte
}
