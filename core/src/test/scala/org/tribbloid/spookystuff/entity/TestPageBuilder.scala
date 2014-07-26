package org.tribbloid.spookystuff.entity

import org.scalatest.{FunSuite, Tag}
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 05/06/14.
 */

class TestPageBuilder extends FunSuite {

  object pageBuilderTag extends Tag("PageBuilder")
  object pageTag extends Tag("Page")

  test("visit and snapshot", pageBuilderTag) {
    val builder = new PageBuilder()
    Visit("http://www.google.com").exe(builder)
    val page = Snapshot().exe(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.startsWith("<!DOCTYPE html>"))
    assert(page.contentStr.contains("<title>Google</title>"))

    assert(page.resolvedUrl.startsWith("http://www.google.ca/?gfe_rd=cr&ei="))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot", pageBuilderTag) {
    val builder = new PageBuilder()
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

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList(0)
    val res2 = resultsList(1)

    val id1 = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40))
    assert(res1.backtrace === id1)
    assert(res1.contentStr.contains("<title>World's Largest Professional Network | LinkedIn</title>"))
    assert(res1.resolvedUrl === "https://www.linkedin.com/")
    assert(res1.alias === "A")

    val id2 = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(res2.backtrace === id2)
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

    val id = Seq[Interactive](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]",40), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"))
    assert(result.backtrace === id)
    assert(result.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(result.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
  }

  test("attributes", pageBuilderTag) {
    val result = PageBuilder.resolveFinal(
      Visit("http://www.amazon.com/"),
      TextInput("input#twotabsearchtextbox", "Lord of the Rings"),
      Submit("input.nav-submit-input"),
      DelayFor("div#resultsCol",50)
    )

    assert(result.attrExist("div#result_0 h3 span.bold","title") === false)
    assert(result.attr1("div#result_0 h3 span.dummy","title") === null)
    assert(result.attr1("div#result_0 h3 span.bold","title") === "")
  }

  test("save", pageTag) {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/"),
      Snapshot().as("T")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.saveLocal()
  }

  test("wget html and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("https://www.google.hk")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.saveLocal()
    assert(page1.text1("title") === "Google")
  }

  test("wget image and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("http://col.stb01.s-msn.com/i/74/A177116AA6132728F299DCF588F794.gif")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.saveLocal()
  }

  test("wget pdf and save", pageTag) {
    val results = PageBuilder.resolve(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf")
    )

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    page1.saveLocal()
  }

}
