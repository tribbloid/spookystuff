package com.tribbloids.spookystuff.utils

object __AmazonS3 {

  /**
    * For Amazon S3:
    *
    * The following character sets are generally safe for use in key names:
    *
    * Alphanumeric characters [0-9a-zA-Z]
    *
    * Special characters !, -, _, ., *, ', (, and )
    *
    * Characters That Might Require Special Handling
    *
    * The following characters in a key name may require additional code handling and will likely need to be URL encoded
    * or referenced as HEX. Some of these are non-printable characters and your browser may not handle them, which will
    * also require special handling:
    *
    * Ampersand ("&") Dollar ("$") ASCII character ranges 00–1F hex (0–31 decimal) and 7F (127 decimal.) 'At' symbol
    * ("@") Equals ("=") Semicolon (";") Colon (":") Plus ("+") Space – Significant sequences of spaces may be lost in
    * some uses (especially multiple spaces) Comma (",") Question mark ("?")
    *
    * Characters to Avoid
    *
    * You should avoid the following characters in a key name because of significant special handling for consistency
    * across all applications.
    *
    * Backslash ("\") Left curly brace ("{") Non-printable ASCII characters (128–255 decimal characters) Caret ("^")
    * Right curly brace ("}") Percent character ("%") Grave accent / back tick ("`") Right square bracket ("]")
    * Quotation marks 'Greater Than' symbol (">") Left square bracket ("[") Tilde ("~") 'Less Than' symbol ("<") 'Pound'
    * character ("#") Vertical bar / pipe ("|")
    *
    * there are 12 characters with special meanings: the backslash \, the caret ^, the dollar sign $, the period or dot
    * ., the vertical bar or pipe symbol |, the question mark ?, the asterisk or star *, the plus sign +, the opening
    * parenthesis (, the closing parenthesis ), and the opening square bracket [, the opening curly brace {, These
    * special characters are often called "metacharacters".
    */
}
