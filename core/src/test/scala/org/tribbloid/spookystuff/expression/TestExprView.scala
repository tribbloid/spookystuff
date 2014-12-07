package org.tribbloid.spookystuff.expression

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Trace, Wget}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.expressions.NamedFunction1

/**
 * Created by peng on 12/3/14.
 */
class TestExprView extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  lazy val page = Trace(
    Wget("http://www.wikipedia.org/") :: Nil
  ).resolve(spooky)
  lazy val row = PageRow(pageLikes = page)
    .select($"title".head.text.>('abc) :: Nil)

  test("symbol as Expr"){
    assert('abc.apply(row) === Some("Wikipedia"))
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