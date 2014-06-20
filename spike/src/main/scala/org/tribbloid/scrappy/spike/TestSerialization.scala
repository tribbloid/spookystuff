package org.tribbloid.scrappy.spike

import java.io.{ObjectOutputStream, ByteArrayOutputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SerializableWritable, SparkEnv}
import org.apache.spark.serializer.SerializationStream

import scala.reflect.ClassTag

/**
 * Created by peng on 08/06/14.
 */

object TestSerialization {

  def main(args: Array[String]) {
    //    val str = "";
    val out = new ByteArrayOutputStream()
    val serOut = new ObjectOutputStream(out)
    serOut.writeObject(new SerializableWritable(new Configuration()))

    val str = out.toString("UTF8")
    serOut.close()

    println(str)
  }
}
