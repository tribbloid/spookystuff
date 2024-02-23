package com.tribbloids.spookystuff.commons.serialization

import ai.acyclic.prover.commons.function.Impl
import ai.acyclic.prover.commons.same.Same
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer, SerializerInstance}

object SerializerEnv {

  case class Ops(conf: SparkConf) {

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

  lazy val apply = Impl { v: SparkConf =>
    Ops(v)
  }
    .cachedBy(Same.ByConstruction.Lookup())

  lazy val Default: Ops = apply(new SparkConf())
}
