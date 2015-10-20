package com.tribbloids.spookystuff

/**
 * Created by peng on 2/21/15.
 */
package object entity {

  type SortKey = Key with SortKeyMixin

  type HiddenKey = Key with HiddenKeyMixin
}
