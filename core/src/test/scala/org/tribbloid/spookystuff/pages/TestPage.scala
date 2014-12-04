package org.tribbloid.spookystuff.pages

import org.apache.hadoop.fs.Path
import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Wget, Visit, Trace, Snapshot}
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.session.Session

/**
 * Created by peng on 10/17/14.
 */
class TestPage extends SpookyEnvSuite {

  val emptyPage: Page = {
    val pb = new Session(spooky)

    pb.getDriver
    Snapshot().doExe(pb).toList(0)
  }

  test("empty page") {
    assert (emptyPage("div.dummy").attrs("href").isEmpty)
    assert (emptyPage("div.dummy").markups.isEmpty)
    assert (emptyPage("div.dummy").isEmpty)
  }

  test("save and load") {

    val results = Trace(
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("s3 save and load") {
    spooky.setRoot(s"s3n://spooky-unit/")

    val results = Trace(
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("wget html, save and load") {

    val results = Trace(
      Wget("https://www.google.hk") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    assert(page1("title").texts.head === "Google")

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("wget image, save and load") {

    val results = Trace(
      Wget("https://www.google.ca/images/srpr/logo11w.png") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("wget pdf, save and load") {

    val results = Trace(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Pages.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }
}
