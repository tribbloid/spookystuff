package com.tribbloids.spookystuff.tests

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

//    def uriContains(contains: String): Boolean = {
//      str.contains(contains) &&
//        str.contains(URLEncoder.encode(contains,"UTF-8"))
//    }
//
//    def assertUriContains(contains: String): Unit = {
//      assert(
//        str.contains(contains) &&
//        str.contains(URLEncoder.encode(contains,"UTF-8")),
//        s"$str doesn't contain either:\n" +
//          s"$contains OR\n" +
//          s"${URLEncoder.encode(contains,"UTF-8")}"
//      )
//    }
  }

  implicit class TestMapView[K, V](map: scala.collection.Map[K, V]) {

    assert(map != null)

    def shouldBe(expected: scala.collection.Map[K, V]): Unit = {

      val messages = expected.toSeq.flatMap {
        tuple =>
          val messageOpt = map.get(tuple._1) match {
            case None =>
              Some(s"${tuple._1} doesn't exist in map")
            case Some(v) =>
              if (v == tuple._2) None
              else Some(s"${tuple._1} mismatch: expected ${tuple._2} =/= actual $v")
          }
          messageOpt
      }

      if (messages.nonEmpty)
        throw new AssertionError("Assertion failure: {\n" + messages.mkString("\n") + "\n}")
    }

    def shouldBe(expected: (K, V)*): Unit = {
      this.shouldBe(Map(expected: _*))
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
