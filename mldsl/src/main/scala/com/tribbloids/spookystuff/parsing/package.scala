package com.tribbloids.spookystuff

package object parsing {

  type StateResolving = (Seq[Char], ParserState) => ParserState
}
