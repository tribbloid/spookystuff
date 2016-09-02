package com.tribbloids.spookystuff.utils

import java.nio.ByteBuffer

import org.apache.hadoop.io.Writable
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SerializableWritable, SparkConf}

class BinaryWritable[T <: Writable](
                                     @transient val _obj: T
                                   ) extends Serializable {

  //TODO: the following are necessary to bypass SPARK-7708, try to remove in the future
  @transient lazy val ser = new JavaSerializer(new SparkConf()).newInstance()

  val binary: Array[Byte] = {
    val wrapped = new SerializableWritable[T](_obj)
    ser.serialize(wrapped).array()
  }

  @transient lazy val value: T = Option(_obj).getOrElse {
    val wrapped = ser.deserialize[SerializableWritable[T]](ByteBuffer.wrap(binary))
    wrapped.value
  }
}

class SerializableUGIWrapper(
                              @transient val _ugi: UserGroupInformation
                            ) extends Serializable {

  val name =  _ugi.getUserName
  val credentials = new BinaryWritable(_ugi.getCredentials)

  @transient lazy val value = Option(_ugi).getOrElse {
    val result = UserGroupInformation.createRemoteUser(name)
    result.addCredentials(credentials.value)
    result
  }
}