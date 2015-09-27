//package com.tribbloids.spookystuff.pipeline
//
//import com.tribbloids.spookystuff.SpookyEnvSuite
//import com.tribbloids.spookystuff.dsl._
//
///**
// * Created by peng on 25/09/15.
// */
//class TestTransformer extends SpookyEnvSuite {
//
//  test("transformer should transform when all column names are given") {
//    val spooky = this.spooky
//    import org.apache.spark.sql.functions._
//    import spooky.dsl._
//    import spooky.sqlContext.implicits._
//
//    //    sc.setCheckpointDir(s"file://${System.getProperty("user.home")}/spooky-local/${this.getClass.getSimpleName}/")
//
//    val transformer = new GoogleSearchTransformer()
//      .setInputCol("_")
//      .setPages(2)
//      .setPageCol("page")
//      .setIndexCol("index")
//
//    val source = sc.parallelize(Seq("Giant Robot"))
//
//    val result = transformer.transform(source)
//      .select(
//        S.uri ~ 'uri,
//        (S \\ "title").text ~ 'title
//      )
//
//    val df = result.toDF(sort = true).persist()
//
//    df.collect().foreach(println)
//    val maxPage = df.agg(max($"page"))
//    assert(maxPage.collect().head.getLong(0) == 2)
//
//    val aggIndex = df
//      .select(explode('index) as 'index0, 'page)
//      .groupBy('page).agg(max('index0), count('index0))
//    aggIndex.collect().foreach {
//      row =>
//        assert(row(1) == row(2).asInstanceOf[Long] - 1)
//    }
//  }
//}
