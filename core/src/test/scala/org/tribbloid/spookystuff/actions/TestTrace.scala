package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.pages.Page

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTrace extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.actions._
  import org.tribbloid.spookystuff.dsl._
  import scala.concurrent.duration._

  test("resolve") {
    val results = Trace(
      Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput").in(40.seconds) ::
        Snapshot().as('A) ::
        TextInput("input#searchInput","Deep learning") ::
        Submit("input.formBtn") ::
        Snapshot().as('B) :: Nil
    ).resolve(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList(0).asInstanceOf[Page]
    val res2 = resultsList(1).asInstanceOf[Page]

    val id1 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::Snapshot().as('C)::Nil)
    assert(res1.uid.backtrace === id1)
    assert(res1.markup.get.contains("<title>Wikipedia</title>"))
    assert(res1.uri === "http://www.wikipedia.org/")
    assert(res1.name === "A")

    val id2 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::TextInput("input#searchInput","Deep learning")::Submit("input.formBtn")::Snapshot().as('D)::Nil)
    assert(res2.uid.backtrace === id2)
    assert(res2.markup.get.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(res2.uri === "http://en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("inject") {

    val t1 = Trace(
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        :: Snapshot().as('a)
        :: Loop (
        ClickNext("button.btn","1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot() ~'b
      ):: Nil
    )

    val t2 = Trace(
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        :: Snapshot().as('c)
        :: Loop (
        ClickNext("button.btn","1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot() ~'d
      ):: Nil
    )

    t1.injectFrom(t2.asInstanceOf[t1.type ])

    assert(t1.outputNames === Set("c","d"))
  }
}
