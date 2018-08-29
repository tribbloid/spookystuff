package com.tribbloids.spookystuff.utils

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}

import scala.reflect.ClassTag

object AssertSerializable {

  def apply[T <: AnyRef: ClassTag](
                                    element: T,
                                    serializers: Seq[Serializer] = {
                                      val conf = new SparkConf()
                                      Seq(
                                        new JavaSerializer(conf),
                                        new KryoSerializer(conf)
                                      )
                                    },
                                    condition: (T, T) => Any = {
                                      (v1: T, v2: T) =>
                                        assert((v1: T) == (v2: T))
                                        assert(v1.toString == v2.toString)
                                        if (!v1.getClass.getCanonicalName.endsWith("$"))
                                          assert(!(v1 eq v2))
                                    }
                                  ): Unit = {

    AssertWeaklySerializable(element, serializers, condition)
  }
}

case class AssertWeaklySerializable[T <: AnyRef: ClassTag](
                                                            element: T,
                                                            serializers: Seq[Serializer] = {
                                                              val conf = new SparkConf()
                                                              Seq(
                                                                new JavaSerializer(conf),
                                                                new KryoSerializer(conf)
                                                              )
                                                            },
                                                            condition: (T, T) => Any = {
                                                              (v1: T, v2: T) => true
                                                            }
                                                          ) {

  serializers.foreach{
    ser =>
      val serInstance = ser.newInstance()
      val serElement = serInstance.serialize(element)
      val element2 = serInstance.deserialize[T](serElement)
      //      assert(!element.eq(element2))
      condition (element, element2)
  }
}
