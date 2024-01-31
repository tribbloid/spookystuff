package com.tribbloids.spookystuff.utils.serialization

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer, SerializerInstance}

case class OfSparkConf(conf: SparkConf) {

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  @transient lazy val _conf: SparkConf = conf
    .registerKryoClasses(Array(classOf[TypeTag[_]]))

  @transient lazy val javaSerializer: JavaSerializer = new JavaSerializer(_conf)
  @transient lazy val javaOverride: () => Some[SerializerInstance] = { // TODO: use singleton?
    () =>
      Some(javaSerializer.newInstance())
  }

  @transient lazy val kryoSerializer: KryoSerializer = new KryoSerializer(_conf)
  @transient lazy val kryoOverride: () => Some[SerializerInstance] = { // TODO: use singleton?
    () =>
      Some(kryoSerializer.newInstance())
  }

  @transient lazy val allSerializers: List[Serializer] = List(javaSerializer, kryoSerializer)
}
