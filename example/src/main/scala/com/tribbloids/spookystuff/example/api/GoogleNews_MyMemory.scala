package com.tribbloids.spookystuff.example.api

import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.{SpookyContext, dsl}

import scala.language.postfixOps

/**
 * Created by peng on 01/08/15.
 */
object GoogleNews_MyMemory extends QueryCore {

  override def doMain(spooky: SpookyContext): Any = {

    import dsl._

    spooky.wget("https://ajax.googleapis.com/ajax/services/search/news?v=1.0&q=nepal")
      .wgetJoin(x"http://api.mymemory.translated.net/get?q=${S\"responseData"\"results"\"content" text}!&langpair=en|fr")
      .select((S\"responseData"\"translatedText" text) ~ 'text)
  }
}
