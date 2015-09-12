package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.pages.Page

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTrace extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  import scala.concurrent.duration._

  test("resolve") {
    val results = (
      Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput").in(40.seconds) ::
        Snapshot().as('A) ::
        TextInput("input#searchInput","Deep learning") ::
        Submit("input.formBtn") ::
        Snapshot().as('B) :: Nil
    ).fetch(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList.head.asInstanceOf[Page]
    val res2 = resultsList(1).asInstanceOf[Page]

    val id1 = Visit("http://www.wikipedia.org") :: WaitFor("input#searchInput") :: Snapshot().as('C) :: Nil
    assert(res1.uid.backtrace === id1)
    assert(res1.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
    assert(res1.uri contains "//www.wikipedia.org")
    assert(res1.name === "A")

    val id2 = Visit("http://www.wikipedia.org") :: WaitFor("input#searchInput") :: TextInput("input#searchInput", "Deep learning") :: Submit("input.formBtn") :: Snapshot().as('D) :: Nil
    assert(res2.uid.backtrace === id2)
    assert(res2.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(res2.uri contains "//en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("inject") {

    val t1 = (
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        :: Snapshot().as('a)
        :: Loop (
        ClickNext("button.btn","1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot() ~'b
      ):: Nil
    )

    val t2 = (
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

  test("dryrun should discard preceding actions when calculating Driverless action's backtrace") {

    val dry = (Delay(10.seconds) +> Wget("http://dum.my")).head.dryrun
    assert(dry.size == 1)
    assert(dry.head == Seq(Wget("http://dum.my")))

    val dry2 = (Delay(10.seconds) +> OAuthV2(Wget("http://dum.my"))).head.dryrun
    assert(dry2.size == 1)
    assert(dry2.head == Seq(OAuthV2(Wget("http://dum.my"))))
  }
}
