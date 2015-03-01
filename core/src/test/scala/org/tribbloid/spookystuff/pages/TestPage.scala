package org.tribbloid.spookystuff.pages

import org.apache.hadoop.fs.Path
import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Snapshot, Trace, Visit, Wget}
import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.session.DriverSession

/**
 * Created by peng on 10/17/14.
 */
class TestPage extends SpookyEnvSuite {

  import dsl._

  test("empty page") {
    val emptyPage: Page = {
      val pb = new DriverSession(spooky)

      Snapshot().doExe(pb).toList(0).asInstanceOf[Page]
    }

    assert (emptyPage.children("div.dummy").attrs("href").isEmpty)
    assert (emptyPage.children("div.dummy").markups.isEmpty)
    assert (emptyPage.children("div.dummy").isEmpty)
  }

  test("save and load") {

    val results = Trace(
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0).asInstanceOf[Page]

    page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(new Path(page1.saved.head))(spooky)

    assert(loadedContent === page1.content)
  }

//  test("s3 save and load") {
//    spooky.setRoot(s"s3n://spooky-unit/")
//
//    val results = Trace(
//      Visit("http://en.wikipedia.org") ::
//        Snapshot().as('T) :: Nil
//    ).resolve(spooky)
//
//    val resultsList = results.toArray
//    assert(resultsList.size === 1)
//    val page1 = resultsList(0)
//
//    val page1Saved = page1.autoSave(spooky,overwrite = true)
//
//    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)
//
//    assert(loadedContent === page1Saved.content)
//  }

  test("wget html, save and load") {

    val results = Trace(
      Wget("http://en.wikipedia.org") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0).asInstanceOf[Page]

    assert(page1.children("title").texts.head startsWith "Wikipedia")

    page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(new Path(page1.saved.head))(spooky)

    assert(loadedContent === page1.content)
  }

  test("wget image, save and load") {

    val results = Trace(
      Wget("https://www.google.ca/images/srpr/logo11w.png") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0).asInstanceOf[Page]

    page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(new Path(page1.saved.head))(spooky)

    assert(loadedContent === page1.content)
  }

  test("wget pdf, save and load") {

    val results = Trace(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0).asInstanceOf[Page]

    page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(new Path(page1.saved.head))(spooky)

    assert(loadedContent === page1.content)
  }

  test("childrenWithSiblings") {
    val page = Trace(
      Wget("http://www.wikipedia.org/") :: Nil
    ).resolve(spooky)(0).asInstanceOf[Page]

    val ranges = page.childrenWithSiblings("a.link-box em", -2 to 1)
    assert(ranges.size === 10)
    val first = ranges.head
    assert(first.size === 4)
    assert(first(0).markup.get.startsWith("<strong"))
    assert(first(1).markup.get.startsWith("<br"))
    assert(first(2).markup.get.startsWith("<em"))
    assert(first(3).markup.get.startsWith("<br"))
  }

  test("childrenWithSiblings with overlapping elimiation") {
    val page = Trace(
      Wget("http://www.wikipedia.org/") :: Nil
    ).resolve(spooky)(0).asInstanceOf[Page]

    val ranges = page.childrenWithSiblings("div.central-featured-lang[lang^=e]", -2 to 2)
    assert(ranges.size === 2)
    val first = ranges.head
    val second = ranges.last
    assert(first.size === 3)
    assert(first(1).attr("lang").get === "en")
    assert(first(2).attr("lang").get === "ru")
    assert(second.size === 4)
    assert(second(0).attr("lang").get === "ru")
    assert(second(1).attr("lang").get === "es")
  }
}
