package com.tribbloids.spookystuff.unused

import org.mapdb.DataInput2

import java.io._

/**
  * Wraps [[DataInput]] into [[InputStream]] see https://github.com/jankotek/mapdb/issues/971
  */
case class DataInput2AsStream(in: DataInput2) extends InputStream {

  override def read(b: Array[Byte], off: Int, len: Int): Int = {

    val srcArray = in.internalByteArray()
    val srcBuffer = in.internalByteBuffer()

    val _len =
      if (srcArray != null) Math.min(srcArray.length, len)
      else if (srcBuffer != null) Math.min(srcBuffer.remaining(), len)
      else len

    val pos = in.getPos
    //      val pos2Opt = Option(in.internalByteBuffer()).map(_.position())

    try {
      in.readFully(b, off, _len)
      _len
    } catch {

      // inefficient way
      case e: RuntimeException =>
        //          Option(in.internalByteBuffer()).foreach { v =>
        //            v.rewind()
        //            v.position(pos2Opt.get)
        //          } // no need, always 0
        in.setPos(pos)

        (off until (off + len)).foreach { i =>
          try {

            val next = in.readByte()
            b.update(i, next)
          } catch {
            case _: EOFException | _: RuntimeException =>
              return i
          }
        }
        len
    }
  }

  override def skip(n: Long): Long = {
    val _n = Math.min(n, Integer.MAX_VALUE)
    // $DELAY$
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
