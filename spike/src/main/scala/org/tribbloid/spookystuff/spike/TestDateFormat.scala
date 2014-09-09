package org.tribbloid.spookystuff.spike

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by peng on 9/9/14.
 */
object TestDateFormat {

  def main(args: Array[String]) {

    val df = new SimpleDateFormat("yyyy-MM-dd-HH")

    val start = df.parse("2014-09-05-00").getTime
    val end = new Date().getTime

    val range = start.to(end, 3600*1000).map(time => df.format(new Date(time)))
  }
}
