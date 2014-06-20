package org.tribbloid.spookystuff.entity

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.broadcast.Broadcast
import org.scalatest.{Tag, FunSuite}
import org.tribbloid.spookystuff.Conf
import scala.collection.JavaConversions._

/**
 * Created by peng on 05/06/14.
 */

class TestPageBuilder extends FunSuite {

  object pageBuilderTag extends Tag("PageBuilder")
  object pageTag extends Tag("Page")

  val conf = new SparkConf().setAppName("MoreLinkedIn")
  conf.setMaster("local[8,5]")
  //    conf.setMaster("local-cluster[2,4,1000]")
  conf.setSparkHome(System.getenv("SPARK_HOME"))
  val jars = SparkContext.jarOfClass(this.getClass).toList
  conf.setJars(jars)
  conf.set("spark.task.maxFailures", "5")
  val sc = new SparkContext(conf)

  Conf.init(sc)

  test("visit and snapshot", pageBuilderTag) {
    val builder = new PageBuilder()
    builder.exe(Visit("http://www.google.com"))
    val page = builder.exe(Snapshot())
    //    val url = builder.getUrl

    assert(page.contentStr.startsWith("<!DOCTYPE html>"))
    assert(page.contentStr.contains("<title>Google</title>"))

    assert(page.resolvedUrl.startsWith("http://www.google.ca/?gfe_rd=cr&ei="))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot", pageBuilderTag) {
    val builder = new PageBuilder()
    builder.exe(Visit("https://www.linkedin.com/"))
    builder.exe(TextInput("input#first","Adam"))
    builder.exe(TextInput("input#last","Muise"))
    builder.exe(Submit("input[name=\"search\"]"))
    val page = builder.exe(Snapshot())
    //    val url = builder.getUrl

    assert(page.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(page.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("resolve", pageBuilderTag) {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      DelayFor("input[name=\"search\"]",40),
      Snapshot().as("A"),
      TextInput("input#first","Adam"),
      TextInput("input#last","Muise"),
      Submit("input[name=\"search\"]"),
      Snapshot().as("B")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 2)
    val res1 = resultsList(0)
    val res2 = resultsList(1)

    val id1 = Seq[Interaction](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40))
    assert(res1.backtrace.toIndexedSeq.toSeq === id1)
    assert(res1.contentStr.contains("<title>World's Largest Professional Network | LinkedIn</title>"))
    assert(res1.resolvedUrl === "https://www.linkedin.com/")
    assert(res1.alias === "A")

    val id2 = Seq[Interaction](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(res2.backtrace.toIndexedSeq.toSeq === id2)
    assert(res2.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(res2.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
    assert(res2.alias === "B")
  }

  test("extract", pageBuilderTag) {
    val result = PageBuilder.resolveFinal(
      Visit("https://www.linkedin.com/"),
      DelayFor("input[name=\"search\"]", 40),
      TextInput("input#first", "Adam"),
      TextInput("input#last", "Muise"),
      Submit("input[name=\"search\"]")
    )

    val id = Seq[Interaction](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(result.backtrace.toIndexedSeq.toSeq === id)
    assert(result.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(result.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
  }

  test("save", pageTag) {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      Snapshot().as("T")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.save()
  }

  test("wget html and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("https://www.google.hk")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.save()
    assert(page1.text1("title") === "Google")
  }

  test("wget image and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("http://col.stb01.s-msn.com/i/74/A177116AA6132728F299DCF588F794.gif")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.save()
  }

  test("wget pdf and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.save()
  }

}
