package org.tribbloid.spookystuff

/**
 * Created by peng on 2/21/15.
 */
package object entity {

  type SortKey = Key with SortKeyHelper

  type HiddenKey = Key with HiddenKeyHelper
}
