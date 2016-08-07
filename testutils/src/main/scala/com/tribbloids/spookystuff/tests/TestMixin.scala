package com.tribbloids.spookystuff.tests

import java.net.URLEncoder

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}

import scala.reflect.ClassTag

/**
  * Created by peng on 17/05/16.
  */
trait TestMixin {

  implicit class TestStringView(str: String) {

    //TODO: use reflection to figure out test name and annotate
    def shouldBe(gd: String = null): Unit = {
      val a = str.split("\n").toList.filterNot(_.replaceAllLiterally(" ","").isEmpty)
        .map(v => ("|" + v).trim.stripPrefix("|"))

      def originalStr = "================================[ORIGINAL]=================================\n" +
        a.mkString("\n") + "\n"

      Option(gd) match {
        case None =>
          println(originalStr)
        case Some(_gd) =>
          val b = _gd.split("\n").toList.filterNot(_.replaceAllLiterally(" ","").isEmpty)
            .map(v => ("|" + v).trim.stripPrefix("|"))
          //          val patch = DiffUtils.diff(a, b)
          //          val unified = DiffUtils.generateUnifiedDiff("Output", "GroundTruth", a, patch, 1)
          //
          //          unified.asScala.foreach(println)
          assert(
            a == b,
            {
              println(originalStr)
              "\n==============================[GROUND TRUTH]===============================\n" +
                b.mkString("\n") + "\n"
            }
          )
      }
    }

    def uriContains(contains: String): Boolean = {
      str.contains(contains) && str.contains(URLEncoder.encode(contains,"UTF-8"))
    }
  }

  def assureSerializable[T <: AnyRef: ClassTag](
                                                 element: T,
                                                 serializers: Seq[Serializer] = {
                                                   val conf = SparkEnv.get.conf
                                                   Seq(
                                                     new JavaSerializer(conf),
                                                     new KryoSerializer(conf)
                                                   )
                                                 },
                                                 condition: (T, T) => Unit = {
                                                   (v1: T, v2: T) =>
                                                     assert((v1: T) == (v2: T))
                                                 }
                                               ): Unit = {

    serializers.foreach{
      ser =>
        val serInstance = ser.newInstance()
        val serElement = serInstance.serialize(element)
        val element2 = serInstance.deserialize[T](serElement)
//        assert(!element.eq(element2))
        condition (element, element2)
        //    assert(element.hashCode() == element2.hashCode())
        assert(element.toString == element2.toString)
    }
  }

  def printSplitter(name: String) = {
    println(s"======================================= $name ===================================")
  }
}
