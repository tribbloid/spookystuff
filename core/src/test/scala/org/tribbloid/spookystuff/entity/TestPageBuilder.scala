package org.tribbloid.spookystuff.entity

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, Tag}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 05/06/14.
 */

class TestPageBuilder extends FunSuite {

  object PB extends Tag("PageBuilder")
  object P extends Tag("Page")

  lazy val conf: SparkConf = new SparkConf().setAppName("dummy").setMaster("local")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy val spooky: SpookyContext = new SpookyContext(sql)

  test("visit and snapshot", PB) {
    val builder = new PageBuilder(new SpookyContext(null: SQLContext))
    Visit("http://en.wikipedia.org").exe(builder)
    val page = Snapshot().exe(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.startsWith("<!DOCTYPE html>"))
    assert(page.contentStr.contains("<title>Wikipedia"))

    assert(page.resolvedUrl.startsWith("http://en.wikipedia.org/wiki/Main_Page"))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot", PB) {
    val builder = new PageBuilder(new SpookyContext(null: SQLContext))
    Visit("https://www.linkedin.com/").exe(builder)
    TextInput("input#first","Adam").exe(builder)
    TextInput("input#last","Muise").exe(builder)
    Submit("input[name=\"search\"]").exe(builder)
    val page = Snapshot().exe(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(page.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("resolve", PB) {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      DelayFor("input[name=\"search\"]",40),
      Snapshot().as("A"),
      TextInput("input#first","Adam"),
      TextInput("input#last","Muise"),
      Submit("input[name=\"search\"]"),
      Snapshot().as("B")
    )(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList(0)
    val res2 = resultsList(1)

    val id1 = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40))
    assert(res1.backtrace === id1)
    assert(res1.contentStr.contains("<title>World's Largest Professional Network | LinkedIn</title>"))
    assert(res1.resolvedUrl === "https://www.linkedin.com/")
//    assert(res1.alias === "A")

    val id2 = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(res2.backtrace === id2)
    assert(res2.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(res2.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
//    assert(res2.alias === "B")
  }

  test("extract", PB) {
    val result = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      DelayFor("input[name=\"search\"]", 40),
      TextInput("input#first", "Adam"),
      TextInput("input#last", "Muise"),
      Submit("input[name=\"search\"]")
    )(spooky)

    val id = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(result(0).backtrace === id)
    assert(result(0).contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(result(0).resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
  }

  test("attributes", PB) {
    val result = PageBuilder.resolve(
      Visit("http://www.amazon.com/"),
      TextInput("input#twotabsearchtextbox", "Lord of the Rings"),
      Submit("input.nav-submit-input"),
      DelayFor("div#resultsCol",50)
    )(spooky)

    assert(result(0).attrExist("div#result_0 h3 span.bold","title") === false)
    assert(result(0).attr1("div#result_0 h3 span.dummy","title") === null)
    assert(result(0).attr1("div#result_0 h3 span.bold","title") === "")
  }

  test("save", P) {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      Snapshot().as("T")
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)
  }

  test("wget html and save", P) {
    val results = PageBuilder.resolve(
      Wget("https://www.google.hk")
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    assert(page1.text1("title") === "Google")
  }

  test("wget image and save", P) {
    val results = PageBuilder.resolve(
      Wget("http://col.stb01.s-msn.com/i/74/A177116AA6132728F299DCF588F794.gif")
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)
  }

  test("wget pdf and save", P) {
    val results = PageBuilder.resolve(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf")
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)
  }

}
