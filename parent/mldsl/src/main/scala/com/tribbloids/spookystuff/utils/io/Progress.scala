package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.AwaitWithHeartbeat.Heartbeat
import org.apache.commons.io.input.ProxyInputStream
import org.apache.commons.io.output.ProxyOutputStream

import java.io.{InputStream, OutputStream}
import java.util.concurrent.atomic.AtomicLong

case class Progress() {

  val indicator: AtomicLong = new AtomicLong(0)

  def ping(): Unit = {
    indicator.incrementAndGet()
  }

  case class WrapIStream(proxy: InputStream) extends ProxyInputStream(proxy) {

    override protected def afterRead(n: Int): Unit = {
      if (n >= 1) ping()
    }
  }

  case class WrapOStream(proxy: OutputStream) extends ProxyOutputStream(proxy) {

    override protected def afterWrite(n: Int): Unit = {
      if (n >= 1) ping()
    }
  }

  private object _ProgressHeartbeat extends Heartbeat {

    // not thread safe
    var previous: Long = 0L

    override def tryEmit(i: Int, remainMil: Long, callerStr: String): Boolean = {
      val result =
        if (indicator.longValue() != previous) true
        else false

      previous = indicator.longValue()
      result
    }
  }

  lazy val defaultHeartbeat: Heartbeat = Heartbeat.WrapWithInfo(_ProgressHeartbeat)
}
