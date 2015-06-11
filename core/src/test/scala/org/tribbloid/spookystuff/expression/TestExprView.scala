package org.tribbloid.spookystuff.expression

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.expressions.NamedFunction1

/**
 * Created by peng on 12/3/14.
 */
class TestExprView extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  lazy val page = (
    Wget("http://www.wikipedia.org/") :: Nil
  ).resolve(spooky).toArray
  lazy val row = PageRow(pageLikes = page)
    .select($"title".head.text ~ 'abc)
    .head

  test("symbol as Expr"){
    assert('abc.apply(row) === Some("Wikipedia"))
  }

  test("defaultAs should not rename an Alias") {
    val renamed = 'abc as 'name1
    assert(renamed.name == "name1")
    val renamed2 = renamed as 'name2
    assert(renamed2.name == "name2")
    val notRenamed = renamed defaultAs 'name2
    assert(notRenamed.name == "name1")
  }

  test("andThen"){
    val fun = 'abc.andThen(_.map(_.toString))
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some("Wikipedia"))

    val fun2 = 'abc.andThen(NamedFunction1(_.map(_.toString),"after"))
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some("Wikipedia"))
  }

  test("andMap"){
    val fun = 'abc.andMap(_.toString)
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some("Wikipedia"))

    val fun2 = 'abc.andMap(_.toString, "after")
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some("Wikipedia"))
  }

  test("andFlatMap"){
    val fun = 'abc.andFlatMap(_.toString.headOption)
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some('W'))

    val fun2 = 'abc.andFlatMap(_.toString.headOption, "after")
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some('W'))
  }
}