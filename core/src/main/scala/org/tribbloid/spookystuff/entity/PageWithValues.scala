package org.tribbloid.spookystuff.entity

import com.google.common.collect.ArrayListMultimap

/**
 * Created by peng on 09/06/14.
 */
case class PageWithValues(
                           val page: Page,
                           val values: ArrayListMultimap[String,String] = ArrayListMultimap.create()
                           )
  extends Serializable