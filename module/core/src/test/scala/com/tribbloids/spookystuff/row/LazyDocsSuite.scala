//package com.tribbloids.spookystuff.row
//
//import com.tribbloids.spookystuff.SpookyEnvSuite
//import com.tribbloids.spookystuff.actions.Wget
//import org.apache.spark.storage.StorageLevel
//
///**
//  * Created by peng on 06/06/16.
//  */
//class LazyDocsSuite extends SpookyEnvSuite {
//
//  import com.tribbloids.spookystuff.dsl._
//
//  val levelsThatPersistDocs = List(
//    "MEMORY_ONLY",
//    "MEMORY_ONLY_2",
//    "MEMORY_AND_DISK",
//    "MEMORY_AND_DISK_2"
//  )
//
//  val levelsThatDontPersistDocs = List(
//    "MEMORY_ONLY_SER",
//    "MEMORY_ONLY_SER_2",
//    "DISK_ONLY",
//    "MEMORY_AND_DISK_SER",
//    "MEMORY_AND_DISK_SER_2"
//  )
//
//  val rdd = sc.parallelize(Seq(
//    LazyDocs(Array(Wget(HTML_URL) ~ 'A)),
//    LazyDocs(Array(Wget(JSON_URL) ~ 'B))
//  ))
//
//
//  levelsThatPersistDocs.foreach{
//    levelStr =>
//      val level = StorageLevel.fromString(levelStr)
//      test(s"Docs can be persisted with $levelStr") {
//
//        val spooky = this.spooky
//        val acc = sc.accumulator(0)
//
//        val loaded = rdd
//          .map {
//            v =>
//              acc += 1
//              v.get(spooky)
//          }
//          .persist(level)
//
//        val r1 = loaded.flatMap(_.docs.map(_.timestamp)).collect()
//        r1.foreach(println)
//
//        assert(acc.value == 2)
//
//        val r2 = loaded.flatMap(_.docs.map(_.timestamp)).collect()
//        r2.foreach(println)
//
//        assert(acc.value == 2)
//      }
//  }
//
//  levelsThatDontPersistDocs.foreach{
//    levelStr =>
//      val level = StorageLevel.fromString(levelStr)
//      test(s"Docs are discarded if persisted with $levelStr") {
//
//        val spooky = this.spooky
//        val acc = sc.accumulator(0)
//
//        val loaded = rdd
//          .map {
//            v =>
//              acc += 1
//              v.get(spooky)
//          }
//          .persist(level)
//
//        val count1 = loaded.map(_.docsOrEmpty.length).reduce(_ + _)
//        assert(count1 == 2)
//
//        val count2 = loaded.map(_.docsOrEmpty.length).reduce(_ + _)
//        assert(count2 == 0)
//      }
//  }
//}
